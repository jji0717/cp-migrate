#pragma once

#include <ifs/ifs_types.h>

__BEGIN_DECLS

struct isi_error;

/**
 * Wrapper over SBT so that we can support large keys( > 128 bits)
 * This implementation hash the long keys and use the hash key as the first
 * key of the B-Tree and use the second key for collision handling.
 * The actual key is stored in the value part to uniquely identify the
 * original key.
 */

struct hsbt_entry {
	bool			valid_;
	struct btree_flags	flags_;
	size_t			key_length_;
	size_t			value_length_;
	void			*key_;
	void			*value_;
};

/**
 * Free the resources associated with an hsbt_entry.
 */
void hsbt_entry_destroy(struct hsbt_entry *entry);

/**
 * Convert from a struct sbt_entry to a struct hsbt_entry.  Resources used by
 * the resulting struct hsbt_entry must be deallocated using
 * hsbt_entry_destroy.
 */
void sbt_entry_to_hsbt_entry(const struct sbt_entry *sbt_ent,
    struct hsbt_entry *hsbt_ent);

/**
 * Open or create an SBT supporting long name
 * @param name[in] the name of the system B-tree
 * @param create[in] indicate if to create or open the b-tree
 * @param error_out[out] the error
 * @return the SBT file descriptor
 */
int hsbt_open(const char *name, bool create, struct isi_error **error_out);

/**
 * close the sbt
 * @param fd[in] the SBT file discritpor
 */
void hsbt_close(int fd);

/**
 * Find the value for the key in the SBT
 * @param fd[in] the SBT file discritpor
 * @param key[in] the key.
 * @param key_len[in] the key size length in bytes
 * @param entry_buf[out], the entry buffer
 * @param entry_size[in/out] on input, specify the entry_buf size, on out,
 *        the actual entry size
 * @param hash_key[out]: the hash key
 * @param error_out[out] the error. If not found, it is not an error.
 * @return if the entry is found, on error it is always false
 */
bool hsbt_get_entry(int fd, const void *key, size_t key_len,
    void *entry_buf, size_t *entry_size,
    btree_key_t *hash_key, struct isi_error **error_out);

/**
 * Add the key-value pair to the SBT
 * @param fd[in] the SBT file discritpor
 * @param key[in] the key.
 * @param key_len[in] the key size length in bytes
 * @param entry_buf[out], the entry buffer
 * @param entry_size[in] on input, specify the entry_buf size
 * @param hash_key[out] the actual b-tree hash key physically used
 * @param error_out[out] the error.
 */
void hsbt_add_entry(int fd, const void *key, size_t key_len,
    const void *entry_buf, size_t entry_size, btree_key_t *hash_key,
    struct isi_error **error_out);

/**
 * Remove a key from the SBT
 * @param fd[in] the SBT file discritpor
 * @param key[in] the key.
 * @param key_len[in] the key size length in bytes
 * @param error_out[out] the error. If not found, it is not an error and
 */
void hsbt_remove_entry(int fd, const void *key, size_t key_len,
    struct isi_error **error_out);

/**
 * Conditional modification. The entry is modified only when value and/or
 * flags are matched.
 * @param fd[in] the SBT file discritpor
 * @param key[in] the key.
 * @param key_len[in] the key size length in bytes
 */
void hsbt_cond_mod_entry(int fd, const void *key, size_t key_len,
    const void *entry_buf, size_t entry_size, struct btree_flags *entry_flags,
    enum btree_cond_mod cm, const void *old_entry_buf,
    size_t old_entry_size, struct btree_flags *old_entry_flags,
    struct isi_error **error_out);

/**
 * This structure is derived from sbt_bulk_entry.
 */
struct hsbt_bulk_entry
{
	struct sbt_bulk_entry bulk_entry;
	/** indicate if this entry from a hash tree
	 * If this field is false, then key and key_len is ignored.
	 * And the proper key shall be set in the key field of bulk_entry.
	 * If is_hash_btree is true, then the key field in bulk_entry is
	 * ignored on input. And on output, they are set.
	 */
	bool is_hash_btree;

	/**
	 * The variable length key
	 */
	const void *key;

	/**
	 * The key length
	 */
	size_t key_len;
};

/**
 * Btree bulk operation this is a wrapper over ifs_sbt_bulk_op.
 * The assumption is at least one B-tree in the operation is a hash btree.
 * If none of the btree is hash btree then the behavior is the same as
 * ifs_sbt_bulk_op.
 */
void hsbt_bulk_op(int num_ops, struct hsbt_bulk_entry *bulk_ops,
    struct isi_error **error_out);

__END_DECLS
