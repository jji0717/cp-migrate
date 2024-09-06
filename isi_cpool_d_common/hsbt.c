#include "hsbt.h"

#include <ifs/ifs_types.h>
#include <ifs/ifs_syscalls.h>
#include <ifs/btree/btree.h>
#include <ifs/sbt/sbt.h>

#include <isi_sbtree/sbtree.h>
#include <isi_util/util_adt.h>
#include <isi_util/isi_hash.h>
#include <isi_util/isi_error.h>
#include <isi_ufp/isi_ufp.h>

/**
 * This routine searches for the key. It also optionally returns the space
 * to insert the key if it is not found. It always returns the hash key.
 * @param fd[in] the SBT file descriptor
 * @param key[in] the key to find
 * @param key_len[in] the key length
 * @param entry_buf[in] the entry buffer to find, if not interested in it
 *        set it to NULL.
 * @param entry_size[in/out] the entry size of the value, if not interested in
 *        it set it to NULL. entry_buf and entry_size must be be both NULL or
 *        not NULL.
 * @param hash_key[in] must not be NULL. on output, it always have the hash
 *        part calculated. The lower second key either contains the collision
 *        key or the actual key of the found entry.
 * @param error[out] isi_system_error if any. If the entry is found and the
 *        entry_size is not large enough, ENOSPC is returned in the error.
 *        The expected entry_size is set on output.
 * @return true if found, false if not found or error other than ENOSPC
 *
 */
static bool
hsbt_find_key_with_space_info(int fd, const void *key, size_t key_len,
    void *entry_buf, size_t *entry_size, btree_key_t *hash_key,
    uint64_t *collision_hint,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	btree_key_t start_key = {{0, 0}}, next_key = {{0, 0}};

	hash_key->keys[0] = isi_hash64(key, key_len, 0);
	hash_key->keys[1] = -1;

	char *buf = NULL;
	size_t buf_size = 0;
	int ec = 0;
	size_t num_entries = 0;
	bool found = false;
	uint64_t collision_space = -1;
	const struct sbt_entry *entry_header = NULL;

	start_key.keys[0] = hash_key->keys[0];
	start_key.keys[1] = 0;

	// TBD: Need to properly lock down the key range

	buf_size = SBT_MAX_ENTRY_SIZE_128 + sizeof(struct sbt_entry);

	buf = malloc(buf_size);

	if (!buf) {
		error = isi_system_error_new(ENOMEM,
		    "Running out of memory, error: %d", ENOMEM);
		goto out;
	}

	while (true) {
		num_entries = 0;
		memset(buf, 0, buf_size);
		ec = ifs_sbt_get_entries(fd, &start_key, &next_key,
		    buf_size, buf, 1, &num_entries);

		if (ec) {
			error = isi_system_error_new(ec,
			    "Could not get sbt entries, error: %d", ec);
			goto out;
		}

		if (num_entries == 0) {
			// end of B-tree reached.
			break;
		}

		entry_header = (const struct sbt_entry *)buf;
		size_t klen = *(size_t *)((struct sbt_entry *)buf + 1);

		if (klen + sizeof(size_t) > SBT_MAX_ENTRY_SIZE_128) {
			// this indicate an error
			error = isi_system_error_new(EIO,
			    "The key in the SBT is too long, corruption "
			    "klen: %lu limit:%lu", klen + sizeof(size_t),
			    SBT_MAX_ENTRY_SIZE_128);
			goto out;
		}

		const void *key_pos = (const char *)buf + sizeof(size_t) +
		    sizeof(struct sbt_entry);

		uint64_t khash = isi_hash64(key_pos, klen, 0);
		if (khash != entry_header->key.keys[0]) {
			// this is a corruption, the read hash does not match
			// the key.
			error = isi_system_error_new(EIO,
			    "The key hashes does not match, corruption");
			goto out;
		}

		if (key_len == klen && !memcmp(key, key_pos, klen)) {

			found = true;
			hash_key->keys[1] = entry_header->key.keys[1];
			if (entry_buf && entry_size) {
				// the actual value size -- minus the key
				// overhead
				size_t value_size = entry_header->size_out -
				    klen - sizeof(size_t);
				if (*entry_size < value_size) {
					error = isi_system_error_new(ENOSPC,
					    "Not enough space for the input "
					    "buffer: insize: %lld, needsize: "
					    "%lld", *entry_size, value_size);
					*entry_size = value_size;
					goto out;
				}
				const void *val_pos = (const char *)key_pos +
				    klen;
				memcpy(entry_buf, val_pos, value_size);
				*entry_size = value_size;
			}

			break;
		}

		if (khash != hash_key->keys[0]) {
			// we have a different hash: not found
			break;
		}

		// this is the collision case
		if (entry_header->key.keys[1] == collision_space + 1) {
			// there is no space between the two, update the
			// elgible collision space
			collision_space = entry_header->key.keys[1];
		}

		start_key = next_key;

		if (!memcmp(&start_key, &btree_zero_key, sizeof(start_key))) {
			// no more keys
			break;
		}
		// otherwise go to the next key
	}

	if (!found) {
		// if a collision hint is given -- make sure we honor that
		hash_key->keys[1] = (collision_hint && *collision_hint >
		     collision_space + 1)? *collision_hint :
		     collision_space + 1;
	}
out:
	isi_error_handle(error, error_out);

	if (buf)
		free(buf);

	return found;
}

/**
 * Allocate and populate the entry buffer. This include both the non-hashed
 * key and the value.
 * The returned buffer must be freed by calling free().
 */
static
void *allocate_value_buffer(const void *key, size_t key_len,
    const void *entry_buf, size_t entry_size, size_t *value_size)
{
	*value_size = sizeof(key_len) + key_len + entry_size;
	void *buf = malloc(*value_size);

	if (!buf)
		return NULL;

	memcpy(buf, &key_len, sizeof(key_len));
	memcpy((char *)buf + sizeof(key_len), key, key_len);
	memcpy((char *)buf + sizeof(key_len) + key_len, entry_buf, entry_size);
	return buf;
}

void
hsbt_add_entry(int fd, const void *key, size_t key_len, const void *entry_buf,
    size_t entry_size, btree_key_t *hash_key, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	char *buf = NULL;
	size_t buf_size = 0;
	int ec = 0;
	bool found = false;
	struct btree_flags flags = {0};
	btree_key_t hkey = {{0, 0}};

	size_t val_size = key_len + sizeof(key_len) + entry_size;
	if (val_size > SBT_MAX_ENTRY_SIZE_128) {
		error = isi_system_error_new(EINVAL, "The key value pair is "
		    "too long key_len: %lld entry_size: %lld", key_len,
		    entry_size);
		goto out;
	}

	found = hsbt_find_key_with_space_info(fd, key, key_len, NULL, NULL,
	    &hkey, NULL, &error);

	if (found) {
		// The entry is already existing, fail it
		error = isi_system_error_new(EINVAL,
		    "The key already exists");
		goto out;
	}

	buf = allocate_value_buffer(key, key_len, entry_buf, entry_size,
	    &buf_size);

	if (!buf) {
		error = isi_system_error_new(ENOMEM,
		    "Running out of memory, error: %d", ENOMEM);
		goto out;
	}

	ec = ifs_sbt_add_entry(fd, &hkey, buf, buf_size, &flags);
	if (ec) {
		error = isi_system_error_new(ec,
		    "Cloud not add sbt entry, error: %d", ec);
		goto out;
	}
out:
	isi_error_handle(error, error_out);

	if (buf)
		free(buf);

	if (hash_key)
		*hash_key = hkey;
}

bool
hsbt_get_entry(int fd, const void *key, size_t key_len,
    void *entry_buf, size_t *entry_size,
    btree_key_t *hash_key, struct isi_error **error_out)
{
	btree_key_t hkey = {{0, 0}};

	bool found = hsbt_find_key_with_space_info(fd, key, key_len, entry_buf,
	    entry_size, &hkey, NULL, error_out);

	if (hash_key)
		*hash_key = hkey;

	return found;
}

void hsbt_remove_entry(int fd, const void *key, size_t key_len,
    struct isi_error **error_out)
{
	btree_key_t hkey = {{0, 0}};
	int ec = 0;
	bool found = hsbt_find_key_with_space_info(fd, key, key_len, NULL,
	    NULL, &hkey, NULL, error_out);

	if (*error_out) {
		isi_error_add_context(*error_out,
		    "Failed to hsbt_find_key_with_space_info");
		return;
	}

	if (!found)
		return;

	ec = ifs_sbt_remove_entry(fd, &hkey);

	if (ec && errno != ENOENT) {
		*error_out = isi_system_error_new(errno,
		    "Error removing the entry, error: %d", errno);
	}
}

void
hsbt_cond_mod_entry(int fd, const void *key, size_t key_len,
    const void *entry_buf, size_t entry_size, struct btree_flags *entry_flags,
    enum btree_cond_mod cm, const void *old_entry_buf,
    size_t old_entry_size, struct btree_flags *old_entry_flags,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	btree_key_t hkey = {{0, 0}};
	int ec = 0;
	size_t new_buf_size = 0, old_buf_size = 0;
	void *new_buf = NULL, *old_buf = NULL;
	hsbt_find_key_with_space_info(fd, key, key_len, NULL,
	    NULL, &hkey, NULL, &error);

	if (error) {
		isi_error_add_context(error,
		    "Failed to hsbt_find_key_with_space_info");
		goto out;
	}

	new_buf = allocate_value_buffer(key, key_len, entry_buf,
	    entry_size, &new_buf_size);

	if (!new_buf) {
		error = isi_system_error_new(ENOMEM,
		    "Running out of memory, error: %d", ENOMEM);
		goto out;
	}

	old_buf = allocate_value_buffer(key, key_len, old_entry_buf,
	    old_entry_size, &old_buf_size);

	if (!old_buf) {
		error = isi_system_error_new(ENOMEM,
		    "Running out of memory, error: %d", ENOMEM);
		goto out;
	}

	// do conditional mod
	ec = ifs_sbt_cond_mod_entry(fd, &hkey, new_buf, new_buf_size,
	    entry_flags, cm, old_buf, old_buf_size, old_entry_flags);

	if (ec) {
		error = isi_system_error_new(errno,
		    "Error doing conditional modification, error: %d", errno);
		goto out;
	}

out:
	if (new_buf) {
		free(new_buf);
		new_buf = NULL;
	}

	if (old_buf) {
		free(old_buf);
		old_buf = NULL;
	}

	isi_error_handle(error, error_out);
}

void
hsbt_bulk_op(int num_ops, struct hsbt_bulk_entry *bulk_ops,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int i = 0;
	int ec = 0;
	struct uint64_uint64_map UINT64_UINT64_MAP_INIT_CLEAN(hash_map);

	if (!num_ops)
		return;

	struct sbt_bulk_entry *bops = (struct sbt_bulk_entry *)malloc(
	    sizeof(struct sbt_bulk_entry) * num_ops);

	if (!bops) {
		error = isi_system_error_new(ENOMEM,
		    "Running out of memory, error: %d", ENOMEM);
		goto out;
	}

	// copy the bulk ops
	for (i = 0; i < num_ops; ++i) {
		memcpy(bops + i, &bulk_ops[i].bulk_entry,
		    sizeof(struct sbt_bulk_entry));
		struct sbt_bulk_entry *bop = bops + i;

		if (bulk_ops[i].is_hash_btree) {
			// set these to NULL
			bop->entry_buf = NULL;
			bop->old_entry_buf = NULL;
		}
	}

	for (i = 0; i < num_ops; ++i) {
		if (bulk_ops[i].is_hash_btree) {
			btree_key_t hkey = {{0, 0}};
			// For this we need to get its new hash key
			uint64_t hash = isi_hash64(bulk_ops[i].key,
			    bulk_ops[i].key_len, 0);

			uint64_t *coll = uint64_uint64_map_find(&hash_map,
			    hash);

			if (coll) {
				// we have a collision, set the next
				// this also updates the value in the map
				++(*coll);
			}

			hsbt_find_key_with_space_info(
			    bulk_ops[i].bulk_entry.fd,
			    bulk_ops[i].key, bulk_ops[i].key_len,
			    NULL, NULL, &hkey, coll, &error);
			if (error) {
				isi_error_add_context(error,
				    "Failed to hsbt_find_key_with_space_info");
				goto out;
			}

			if (!coll) {
				// add this to the hash map
				uint64_uint64_map_add(&hash_map, hkey.keys[0],
				    &hkey.keys[1]);
			}

			bops[i].key = hkey;

			size_t entry_size = 0;
			bops[i].entry_buf = allocate_value_buffer(
			    bulk_ops[i].key,
			    bulk_ops[i].key_len,
			    bulk_ops[i].bulk_entry.entry_buf,
			    bulk_ops[i].bulk_entry.entry_size,
			    &entry_size);
			if (!bops[i].entry_buf) {
				error = isi_system_error_new(ENOMEM,
				    "Running out of memory, error: %d",
				    ENOMEM);
				goto out;
			}
			bops[i].entry_size = entry_size;

			entry_size = 0;
			bops[i].old_entry_buf = allocate_value_buffer(
			    bulk_ops[i].key,
			    bulk_ops[i].key_len,
			    bulk_ops[i].bulk_entry.old_entry_buf,
			    bulk_ops[i].bulk_entry.old_entry_size,
			    &entry_size);
			if (!bops[i].old_entry_buf) {
				error = isi_system_error_new(ENOMEM,
				    "Running out of memory, error: %d",
				    ENOMEM);
				goto out;
			}
			bops[i].old_entry_size = entry_size;
		}
	}

	// now do the bulk operation

	ec = ifs_sbt_bulk_op(num_ops, bops);

	if (ec) {
		error = isi_system_error_new(errno,
		    "Error doing bulk operations, error: %d", errno);
		goto out;
	}

out:
	isi_error_handle(error, error_out);

	if (bops) {
		for (i = 0; i < num_ops; ++i) {
			struct sbt_bulk_entry *bop = bops + i;

			if (bulk_ops[i].is_hash_btree) {
				// set these to NULL
				if (bop->entry_buf) {
					free(bop->entry_buf);
					bop->entry_buf = NULL;
				}

				if (bop->old_entry_buf) {
					free(bop->old_entry_buf);
					bop->old_entry_buf = NULL;
				}
			}
		}

		free(bops);
		bops = NULL;
	}
}

void
hsbt_entry_destroy(struct hsbt_entry *entry)
{
	ASSERT(entry != NULL);

	free(entry->key_);
	entry->key_ = NULL;

	free(entry->value_);
	entry->value_ = NULL;

	entry->valid_ = false;
}

void
sbt_entry_to_hsbt_entry(const struct sbt_entry *sbt_ent,
    struct hsbt_entry *hsbt_ent)
{
	ASSERT(sbt_ent != NULL);
	ASSERT(hsbt_ent != NULL);

	const void *temp_ptr = NULL;

	// flags
	hsbt_ent->flags_ = sbt_ent->flags;

	// key length
	temp_ptr = sbt_ent->buf;
	hsbt_ent->key_length_ = *((size_t *)temp_ptr);

	// value length
	hsbt_ent->value_length_ = sbt_ent->size_out -
	    sizeof hsbt_ent->key_length_ - hsbt_ent->key_length_;

	// key
	temp_ptr = (char *)temp_ptr + sizeof hsbt_ent->key_length_;
	hsbt_ent->key_ = malloc(hsbt_ent->key_length_);
	ASSERT(hsbt_ent->key_ != NULL);
	memcpy(hsbt_ent->key_, temp_ptr, hsbt_ent->key_length_);

	// value
	temp_ptr = (char *)temp_ptr + hsbt_ent->key_length_;
	hsbt_ent->value_ = malloc(hsbt_ent->value_length_);
	ASSERT(hsbt_ent->value_ != NULL);
	memcpy(hsbt_ent->value_, temp_ptr, hsbt_ent->value_length_);

	// valid
	hsbt_ent->valid_ =
	    (sizeof hsbt_ent->key_length_ + hsbt_ent->key_length_ <=
	    SBT_MAX_ENTRY_SIZE_128) &&
	    (sbt_ent->key.keys[0] == isi_hash64(hsbt_ent->key_,
	    hsbt_ent->key_length_, 0));
}
