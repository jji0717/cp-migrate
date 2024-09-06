#include <ifs/ifs_types.h>
#include <ifs/ifs_syscalls.h>
#include <ifs/btree/btree.h>
#include <ifs/sbt/sbt.h>

#include <isi_sbtree/sbtree.h>
#include <isi_util/util_adt.h>
#include <isi_util/isi_hash.h>
#include <isi_util/isi_error.h>
#include <isi_ufp/isi_ufp.h>

#include "isi_cbm_coi_sbt.h"

#define CMO_HASH_BITS 48

const static uint64_t collision_mask = (1 << (64 - CMO_HASH_BITS)) - 1;

static void
hash_coi_key(const struct coi_sbt_key *key, btree_key_t *hash_key)
{
	uint64_t khash = coi_sbt_get_hashed_key(key);

	size_t shift = 64 - CMO_HASH_BITS;

	hash_key->keys[0] = khash | (key->snapid_ >> CMO_HASH_BITS);
	hash_key->keys[1] = key->snapid_ << shift;
}

uint64_t
coi_sbt_get_hashed_key(const struct coi_sbt_key *key)
{
	size_t shift = 64 - CMO_HASH_BITS;

	uint64_t khash = isi_hash64(key, sizeof(key->high_) + sizeof(key->low_),
	    0);
	khash <<= shift;
	return khash;
}

/**
 * Compare the two hash key ignoring the collision part
 */
static bool
equal_coi_hash_key(const btree_key_t *key1, const btree_key_t *key2)
{
	return (key1->keys[0] == key2->keys[0] &&
	    (key1->keys[1] & ~collision_mask) ==
	    (key2->keys[1] & ~collision_mask));
}

/**
 * This routine searches for the key or its next or previous versions.
 * It also optionally returns the space
 * to insert the key if it is not found. It always returns the hash key.
 * @param fd[in] the SBT file descriptor
 * @param key[in] the key to find
 * @param entry_buf[in] the entry buffer to find, if not interested in it
 *        set it to NULL.
 * @param entry_size[in/out] the entry size of the value, if not interested in
 *        it set it to NULL. entry_buf and entry_size must be be both NULL or
 *        not NULL.
 * @param hash_key[out] must not be NULL. on output, it always have the hash
 *        part calculated.
 * @param sdir[in] search direction, at the given key or the next
 * @param error[out] isi_system_error if any. If the entry is found and the
 *        entry_size is not large enough, ENOSPC is returned in the error.
 *        The expected entry_size is set on output.
 * @return true if found, false if not found or error other than ENOSPC
 *
 */
static bool
coi_sbt_find_key_with_space_info(int fd, const struct coi_sbt_key *key,
    void *entry_buf, size_t *entry_size, btree_key_t *hash_key,
    uint64_t *collision_hint,
    enum SBT_SEARCH_DIR sdir,
    struct coi_sbt_key *out_key,
    struct isi_error **error_out)
{
	const static uint64_t MAX_UINT64 = 0xffffffffffffffffU;
	struct isi_error *error = NULL;
	btree_key_t start_key = {{0, 0}};
	struct coi_sbt_key s_key = {0};

	size_t klen = sizeof(struct coi_sbt_key);

	char *buf = NULL;
	size_t buf_size = 0;
	int ec = 0;
	size_t num_entries = 0;
	bool found = false;
	uint64_t collision_space = -1;
	struct sbt_entry entry;
	s_key = *key;
	uint64_t hkey_in = coi_sbt_get_hashed_key(key);
	bool end = false;

	switch (sdir) {
	case SD_NEXT:
		++s_key.snapid_;
		break;
	case SD_PREV:
		if (s_key.snapid_ == 0)
			goto out;
		--s_key.snapid_;
		break;
	case SD_LAST:
		s_key.snapid_ = MAX_UINT64;
		break;
	default:
		break;
	}
	hash_coi_key(&s_key, hash_key);

	start_key = *hash_key;

	if (sdir == SD_LAST) {
		// make sure we start from the largest collision value too
		start_key.keys[1] = MAX_UINT64;
	} else if (sdir == SD_PREV) {
		// make sure we start from the largest collision value for this version
		start_key.keys[1] |= collision_mask;
	}

	buf_size = SBT_MAX_ENTRY_SIZE_128 + sizeof(struct sbt_entry);

	buf = malloc(buf_size);

	if (!buf) {
		error = isi_system_error_new(ENOMEM,
		    "Running out of memory, error: %d", ENOMEM);
		goto out;
	}

	while (true) {
		size_t size_out = 0;
		num_entries = 0;
		memset(buf, 0, buf_size);
		if (sdir == SD_AT || sdir == SD_NEXT) {
			ec = ifs_sbt_get_entry_at_or_after(fd, &start_key,
			    &entry.key, buf, buf_size, &entry.flags,
			    &size_out);
		} else {
			ec = ifs_sbt_get_entry_at_or_before(fd, &start_key,
			    &entry.key, buf, buf_size, &entry.flags,
			    &size_out);
		}

		if (ec) {
			if (errno == ENOENT)
				break;
			error = isi_system_error_new(errno,
			    "Could not get sbt entries, error: %d", errno);
			goto out;
		}

		entry.size_out = (uint32_t)size_out;
		end = entry.size_out == 0;

		if (end) {
			// end of B-tree reached.
			break;
		}

		const struct coi_sbt_key *key_pos = NULL;
		key_pos = (const struct coi_sbt_key *)buf;

		btree_key_t khash;
		hash_coi_key(key_pos, &khash);
		if (!equal_coi_hash_key(&khash, &entry.key)) {
			// this is a corruption, the read hash does not match
			// the key.
			error = isi_system_error_new(EIO,
			    "The key hashes does not match, corruption: %ld "
			    "%ld", khash.keys[0],  entry.key.keys[0]);
			goto out;
		}

		uint64_t hkey = coi_sbt_get_hashed_key(key_pos);
		if (hkey != hkey_in) {
			// we have a different hash value: not found
			break;
		}

		if (sdir == SD_AT && !memcmp(key, key_pos, klen)) {
			found = true;
		} else if (sdir == SD_NEXT || sdir == SD_PREV ||
		    sdir == SD_LAST) {
			if (key_pos->high_ == key->high_ &&
			    key_pos->low_ == key->low_) {
				// version of the same object found:
				found = true;
			}
		}

		if (found) {
			hash_key->keys[0] = entry.key.keys[0];
			hash_key->keys[1] = entry.key.keys[1];
		}

		if (found && entry_buf && entry_size) {
			// the actual value size -- minus the key
			// overhead
			size_t value_size = entry.size_out - klen;
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

		if (found && out_key)
			*out_key = *key_pos;
		if (found)
			break;

		if (sdir == SD_AT) {
			// this is the collision case, get the collision value
			uint64_t collision = entry.key.keys[1] &
			    collision_mask;

			if (collision == collision_space + 1) {
				// there is no space between the two, update the
				// eligible collision space
				collision_space = collision;
			}
		}

		if (sdir == SD_AT || sdir == SD_NEXT) {
			start_key = entry.key;
			if (start_key.keys[1] != MAX_UINT64)
				++start_key.keys[1];
			else if (start_key.keys[0] != MAX_UINT64) {
				++start_key.keys[0];
				start_key.keys[1] = 0;
			}
			else
				break;
		} else {
			start_key = entry.key;
			if (start_key.keys[1] != 0)
				--start_key.keys[1];
			else if (start_key.keys[0] != 0) {
				--start_key.keys[0];
				start_key.keys[1] = MAX_UINT64;
			}
			else
				break;
		}

		// get the new start key's hash key part
		hkey = start_key.keys[0] & ~collision_mask;
		if (hkey != hkey_in) {
			// we are looking at a different hash key, not found
			break;
		}

		if (!memcmp(&start_key, &btree_zero_key, sizeof(start_key))) {
			// no more keys
			break;
		}
		// otherwise go to the next key
	}

	if (sdir == SD_AT && !found) {
		// if a collision hint is given -- make sure we honor that
		uint64_t collision = (collision_hint && *collision_hint >
		     collision_space + 1) ? *collision_hint :
		     collision_space + 1;
		hash_key->keys[1] &=  ~collision_mask;
		hash_key->keys[1] |= collision;
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
void *allocate_value_buffer(const struct coi_sbt_key *key,
    const void *entry_buf, size_t entry_size, size_t *value_size)
{
	size_t key_len = sizeof(*key);
	*value_size = key_len + entry_size;
	void *buf = malloc(*value_size);

	if (!buf)
		return NULL;

	memcpy((char *)buf, key, key_len);
	memcpy((char *)buf + key_len, entry_buf, entry_size);
	return buf;
}

void
coi_sbt_add_entry(int fd, const struct coi_sbt_key *key, const void *entry_buf,
    size_t entry_size, btree_key_t *hash_key, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	char *buf = NULL;
	size_t buf_size = 0;
	int ec = 0;
	bool found = false;
	struct btree_flags flags = {0};
	btree_key_t hkey = {{0, 0}};
	size_t key_len = sizeof(*key);


	size_t val_size = key_len + sizeof(key_len) + entry_size;
	if (val_size > SBT_MAX_ENTRY_SIZE_128) {
		error = isi_system_error_new(EINVAL, "The key value pair is "
		    "too long key_len: %lld entry_size: %lld", key_len,
		    entry_size);
		goto out;
	}

	found = coi_sbt_find_key_with_space_info(fd, key, NULL, NULL,
	    &hkey, NULL, SD_AT, NULL, &error);

	if (found) {
		// The entry is already existing, fail it
		error = isi_system_error_new(EINVAL,
		    "The key already exists");
		goto out;
	}

	buf = allocate_value_buffer(key, entry_buf, entry_size,
	    &buf_size);

	if (!buf) {
		error = isi_system_error_new(ENOMEM,
		    "Running out of memory, error: %d", ENOMEM);
		goto out;
	}

	ec = ifs_sbt_add_entry(fd, &hkey, buf, buf_size, &flags);
	if (ec) {
		error = isi_system_error_new(errno,
		    "Cloud not add sbt entry, error: %d", errno);
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
coi_sbt_get_entry(int fd, const struct coi_sbt_key *key,
    void *entry_buf, size_t *entry_size,
    btree_key_t *hash_key, struct isi_error **error_out)
{
	btree_key_t hkey = {{0, 0}};

	bool found = coi_sbt_find_key_with_space_info(fd, key, entry_buf,
	    entry_size, &hkey, NULL, SD_AT, NULL, error_out);

	if (hash_key)
		*hash_key = hkey;

	return found;
}

bool
coi_sbt_get_next_version(int fd, const struct coi_sbt_key *key,
    void *entry_buf, size_t *entry_size,
    btree_key_t *hash_key, struct coi_sbt_key *out_key,
    struct isi_error **error_out)
{
	btree_key_t hkey = {{0, 0}};

	bool found = coi_sbt_find_key_with_space_info(fd, key, entry_buf,
	    entry_size, &hkey, NULL, SD_NEXT, out_key, error_out);

	if (hash_key)
		*hash_key = hkey;

	return found;
}

bool
coi_sbt_get_prev_version(int fd, const struct coi_sbt_key *key,
    void *entry_buf, size_t *entry_size,
    btree_key_t *hash_key, struct coi_sbt_key *out_key,
    struct isi_error **error_out)
{
	btree_key_t hkey = {{0, 0}};

	bool found = coi_sbt_find_key_with_space_info(fd, key, entry_buf,
	    entry_size, &hkey, NULL, SD_PREV, out_key, error_out);

	if (hash_key)
		*hash_key = hkey;

	return found;
}

bool
coi_sbt_get_last_version(int fd, const struct coi_sbt_key *key,
    void *entry_buf, size_t *entry_size,
    btree_key_t *hash_key, struct coi_sbt_key *out_key,
    struct isi_error **error_out)
{
	btree_key_t hkey = {{0, 0}};

	bool found = coi_sbt_find_key_with_space_info(fd, key, entry_buf,
	    entry_size, &hkey, NULL, SD_LAST, out_key, error_out);

	if (hash_key)
		*hash_key = hkey;

	return found;
}

void coi_sbt_remove_entry(int fd, const struct coi_sbt_key *key,
    struct isi_error **error_out)
{
	btree_key_t hkey = {{0, 0}};
	int ec = 0;
	bool found = coi_sbt_find_key_with_space_info(fd, key, NULL,
	    NULL, &hkey, NULL, SD_AT, NULL, error_out);

	if (*error_out) {
		isi_error_add_context(*error_out,
		    "Failed to coi_sbt_find_key_with_space_info");
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
coi_sbt_cond_mod_entry(int fd, const struct coi_sbt_key *key,
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
	coi_sbt_find_key_with_space_info(fd, key, NULL,
	    NULL, &hkey, NULL, SD_AT, NULL, &error);

	if (error) {
		isi_error_add_context(error,
		    "Failed to coi_sbt_find_key_with_space_info");
		goto out;
	}

	new_buf = allocate_value_buffer(key, entry_buf,
	    entry_size, &new_buf_size);

	if (!new_buf) {
		error = isi_system_error_new(ENOMEM,
		    "Running out of memory, error: %d", ENOMEM);
		goto out;
	}

	old_buf = allocate_value_buffer(key, old_entry_buf,
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
coi_sbt_bulk_op(int num_ops, struct coi_sbt_bulk_entry *bulk_ops,
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

		if (bulk_ops[i].is_hash_btree) {
			struct sbt_bulk_entry *bop = bops + i;
			// set these to NULL
			bop->entry_buf = NULL;
			bop->old_entry_buf = NULL;
		}
	}

	for (i = 0; i < num_ops; ++i) {
		if (bulk_ops[i].is_hash_btree) {
			btree_key_t hkey = {{0, 0}};
			// For this we need to get its new hash key
			hash_coi_key(bulk_ops[i].key, &hkey);

			uint64_t *coll = uint64_uint64_map_find(&hash_map,
			    hkey.keys[0]);

			if (coll) {
				// we have a collision, set the next
				// this also updates the value in the map
				++(*coll);
			}

			coi_sbt_find_key_with_space_info(
			    bulk_ops[i].bulk_entry.fd,
			    bulk_ops[i].key,
			    NULL, NULL, &hkey, coll, SD_AT, NULL, &error);
			if (error) {
				isi_error_add_context(error,
				    "Failed to coi_sbt_find_key_with_space_info");
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
coi_sbt_entry_destroy(struct coi_sbt_entry *entry)
{
	ASSERT(entry != NULL);

	free(entry->value_);
	entry->value_ = NULL;

	entry->valid_ = false;
}

void
sbt_entry_to_coi_sbt_entry(const struct sbt_entry *sbt_ent,
    struct coi_sbt_entry *coi_sbt_ent)
{
	ASSERT(sbt_ent != NULL);
	ASSERT(coi_sbt_ent != NULL);

	const void *temp_ptr = NULL;
	size_t key_len = sizeof(struct coi_sbt_key);

	// flags
	coi_sbt_ent->flags_ = sbt_ent->flags;

	// key length
	temp_ptr = sbt_ent->buf;

	// value length
	coi_sbt_ent->value_length_ = sbt_ent->size_out - key_len;

	// key
	temp_ptr = (char *)temp_ptr;
	memcpy(&coi_sbt_ent->key_, temp_ptr, key_len);

	// value
	temp_ptr = (char *)temp_ptr + key_len;
	coi_sbt_ent->value_ = malloc(coi_sbt_ent->value_length_);
	ASSERT(coi_sbt_ent->value_ != NULL);
	memcpy(coi_sbt_ent->value_, temp_ptr, coi_sbt_ent->value_length_);

	btree_key_t khash;
	hash_coi_key(&coi_sbt_ent->key_, &khash);

	// valid
	coi_sbt_ent->valid_ =
	    (sizeof key_len + key_len <=
	    SBT_MAX_ENTRY_SIZE_128) &&
	    equal_coi_hash_key(&sbt_ent->key, &khash);
}

bool coi_sbt_has_entries(int fd, struct isi_error **error_out)
{
	bool have_entry = false;
	struct isi_error *error = NULL;

	int ec = 0;
	btree_key_t start_key = {{0, 0}};
	struct sbt_entry entry;
	const size_t buf_size =
	    SBT_MAX_ENTRY_SIZE_128 + sizeof(struct sbt_entry);
	char buf[buf_size];
	size_t size_out = 0;

	ec = ifs_sbt_get_entry_at_or_after(fd, &start_key,
	    &entry.key, buf, buf_size, &entry.flags, &size_out);

	// No error - must have gotten an entry
	if (ec == 0) {
		have_entry = true;
		goto out;
	}

	if (errno == ENOENT)
		goto out;

	error = isi_system_error_new(errno, "Could not read from SBT");

 out:

	isi_error_handle(error, error_out);

	return have_entry;
}