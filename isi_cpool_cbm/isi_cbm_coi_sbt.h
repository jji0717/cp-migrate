#pragma once

#include <ifs/ifs_types.h>

__BEGIN_DECLS

struct isi_error;

/**
 * Special implementation for SBT backstore for COI.
 * This implementation hash the object id prefix part (48 bit) and put the
 * snapid part in the SBT lower part. The remaining 16 bit in the higher part
 * is used for collision handling. The goal of this scheme is to
 * 1. Satisfy the 128 SBT key size limitation
 * 2. Make the entries of the CMO versions collocated in the SBT
 */

/**
 * COI sbt key
 */
struct coi_sbt_key {
	uint64_t		high_;
	uint64_t		low_;
	uint64_t		snapid_;
}__packed;

/**
 * COI sbt entry
 */
struct coi_sbt_entry {
	bool			valid_;
	struct btree_flags	flags_;
	struct coi_sbt_key	key_;
	size_t			value_length_;
	void			*value_;
};

/**
 * COI SBT search direction
 */
enum SBT_SEARCH_DIR {
	SD_AT,
	SD_NEXT,
	SD_PREV,
	SD_LAST
};

uint64_t coi_sbt_get_hashed_key(const struct coi_sbt_key *key);

/**
 * Free the resources associated with an coi_sbt_entry.
 */
void coi_sbt_entry_destroy(struct coi_sbt_entry *entry);

/**
 * Convert from a struct sbt_entry to a struct coi_sbt_entry.  Resources used by
 * the resulting struct coi_sbt_entry must be deallocated using
 * coi_sbt_entry_destroy.
 */
void sbt_entry_to_coi_sbt_entry(const struct sbt_entry *sbt_ent,
    struct coi_sbt_entry *coi_sbt_ent);

/**
 * Find the value for the key in the SBT
 * @param fd[in] the SBT file descriptor
 * @param key[in] the key.
 * @param entry_buf[out], the entry buffer
 * @param entry_size[in/out] on input, specify the entry_buf size, on out,
 *        the actual entry size
 * @param hash_key[out]: the hash key
 * @param error_out[out] the error. If not found, it is not an error.
 * @return if the entry is found, on error it is always false
 */
bool coi_sbt_get_entry(int fd, const struct coi_sbt_key *key,
    void *entry_buf, size_t *entry_size,
    btree_key_t *hash_key, struct isi_error **error_out);

/**
 * Find the value and the key for the next version of the key in the SBT
 * @param fd[in] the SBT file descriptor
 * @param key[in] the key.
 * @param entry_buf[out], the entry buffer
 * @param entry_size[in/out] on input, specify the entry_buf size, on out,
 *        the actual entry size
 * @param hash_key[out]: the hash key
 * @param out_key[out]: the key for the next version if found
 * @param error_out[out] the error. If not found, it is not an error.
 * @return if the entry is found, on error it is always false
 */
bool coi_sbt_get_next_version(int fd, const struct coi_sbt_key *key,
    void *entry_buf, size_t *entry_size,
    btree_key_t *hash_key, struct coi_sbt_key *out_key,
    struct isi_error **error_out);

/**
 * Find the value and the key for the previous version of the key in the SBT
 * @param fd[in] the SBT file descriptor
 * @param key[in] the key.
 * @param entry_buf[out], the entry buffer
 * @param entry_size[in/out] on input, specify the entry_buf size, on out,
 *        the actual entry size
 * @param hash_key[out]: the hash key
 * @param out_key[out]: the key for the previous version if found
 * @param error_out[out] the error. If not found, it is not an error.
 * @return if the entry is found, on error it is always false
 */
bool coi_sbt_get_prev_version(int fd, const struct coi_sbt_key *key,
    void *entry_buf, size_t *entry_size,
    btree_key_t *hash_key, struct coi_sbt_key *out_key,
    struct isi_error **error_out);

/**
 * Find the value and the key for the last version of the key in the SBT
 * @param fd[in] the SBT file descriptor
 * @param key[in] the key.
 * @param entry_buf[out], the entry buffer
 * @param entry_size[in/out] on input, specify the entry_buf size, on out,
 *        the actual entry size
 * @param hash_key[out]: the hash key
 * @param out_key[out]: the key for the last version if found
 * @param error_out[out] the error. If not found, it is not an error.
 * @return if the entry is found, on error it is always false
 */
bool coi_sbt_get_last_version(int fd, const struct coi_sbt_key *key,
    void *entry_buf, size_t *entry_size,
    btree_key_t *hash_key, struct coi_sbt_key *out_key,
    struct isi_error **error_out);

/**
 * Add the key-value pair to the SBT
 * @param fd[in] the SBT file descriptor
 * @param key[in] the key.
 * @param entry_buf[out], the entry buffer
 * @param entry_size[in] on input, specify the entry_buf size
 * @param hash_key[out] the actual b-tree hash key physically used
 * @param error_out[out] the error.
 */
void coi_sbt_add_entry(int fd, const struct coi_sbt_key *key,
    const void *entry_buf, size_t entry_size, btree_key_t *hash_key,
    struct isi_error **error_out);

/**
 * Remove a key from the SBT
 * @param fd[in] the SBT file descriptor
 * @param key[in] the key.
 * @param error_out[out] the error. If not found, it is not an error and
 */
void coi_sbt_remove_entry(int fd, const struct coi_sbt_key *key,
    struct isi_error **error_out);

/**
 * Conditional modification. The entry is modified only when value and/or
 * flags are matched.
 * @param fd[in] the SBT file descriptor
 * @param key[in] the key.
 */
void coi_sbt_cond_mod_entry(int fd, const struct coi_sbt_key *key,
    const void *entry_buf, size_t entry_size, struct btree_flags *entry_flags,
    enum btree_cond_mod cm, const void *old_entry_buf,
    size_t old_entry_size, struct btree_flags *old_entry_flags,
    struct isi_error **error_out);

/**
 * Returns true if there are any entries in the SBT and false otherwise
 * @param fd[in] the SBT file descriptor
 * @param error_out Error if any (Note, ENOENT does not manifest as an error)
 */
bool coi_sbt_has_entries(int fd, struct isi_error **error_out);

/**
 * This structure is derived from sbt_bulk_entry.
 */
struct coi_sbt_bulk_entry
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
 * The assumption is at least one B-tree in the operation is a COI hash btree.
 * If none of the btree is hash btree then the behavior is the same as
 * ifs_sbt_bulk_op.
 */
void coi_sbt_bulk_op(int num_ops, struct coi_sbt_bulk_entry *bulk_ops,
    struct isi_error **error_out);

__END_DECLS
