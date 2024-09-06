#include <assert.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <netdb.h>
#include <arpa/inet.h>

#include <ifs/ifs_syscalls.h>
#include <ifs/ifs_types.h>
#include <isi_config/array.h>
#include <isi_ilog/ilog.h>
#include <isi_sbtree/sbtree.h>
#include <isi_util/isi_error.h>
#include <isi_util_cpp/scoped_lock.h>
#include <isi_flexnet/isi_flexnet.h>
#include <isi_pools_fsa_gcfg/smartpools_util.h>

#include "ifs_cpool_flock.h"
#include "task.h"

#include "cpool_d_common.h"
#include <isi_cloud_common/isi_cpool_version.h>

// parent directory for CloudPools SBTs
static const char *SBT_DIR = "isi_cpool_d_sbt";

static uint8_t s_handling_node_id[IFSCONFIG_GUID_SIZE] = { };
static bool s_handling_node_id_initialized = false;
static pthread_mutex_t s_handling_node_id_init_mutex =
    PTHREAD_MUTEX_INITIALIZER;
static int s_node_devid = -1;
static int s_node_lnn = -1;
static pthread_mutex_t s_node_devid_init_mutex = PTHREAD_MUTEX_INITIALIZER;
static uint32_t GET_ENTRY_CTX_VALID_MAGIC = 0xfeedface;

static void
array_byte_fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args,
    union fmt_conv_arg arg)
{
	const uint8_t *value = (const uint8_t *) arg.ptrlen.ptr;
	size_t size = arg.ptrlen.len;
	int i;

	for (i = 0; i < size; i++) {
		fmt_print(fmt, "%x", (value[i] >> 0) & 0xf);
		fmt_print(fmt, "%x", (value[i] >> 4) & 0xf);
	}
}

struct fmt_conv_ctx
array_byte_fmt(const uint8_t *value, size_t size)
{
	struct fmt_conv_ctx ctx = {
		.func = array_byte_fmt_conv,
		.arg = { .ptrlen = { value, size } }
	};

	return ctx;
}

static void
get_sbt_entry_ctx_init(struct get_sbt_entry_ctx **ctx)
{
	ASSERT(ctx != NULL);

	*ctx = (struct get_sbt_entry_ctx *)
	    malloc(sizeof(struct get_sbt_entry_ctx));
	ASSERT(*ctx != NULL);

	(*ctx)->magic_ = GET_ENTRY_CTX_VALID_MAGIC;
	(*ctx)->first_key_ = NULL;
	(*ctx)->looped_ = false;
}

void
get_sbt_entry_ctx_destroy(struct get_sbt_entry_ctx **ctx)
{
	ASSERT(ctx != NULL);

	if (*ctx != NULL) {
		ASSERT((*ctx)->magic_ == GET_ENTRY_CTX_VALID_MAGIC);

		if ((*ctx)->first_key_ != NULL)
			free((*ctx)->first_key_);

		free(*ctx);
		*ctx = NULL;
	}
}

static int
get_random_int_between(int low, int high)
{
	ASSERT(low <= high);

	if (low == high)
		return low;

	int ret_val = low;

	static bool been_here = false;
	if (!been_here) {
		srand(time(0));
		been_here = true;
	}

	ret_val = rand()%(high + 1 - low) + low;

	ASSERT(ret_val >= low && ret_val <= high);

	return ret_val;
}

struct sbt_entry *
get_sbt_entry(int sbt_fd, btree_key_t *start_key, char **entry_buf,
    struct get_sbt_entry_ctx **ctx, struct isi_error **error_out)
{
	return get_sbt_entry_randomize(sbt_fd, start_key, entry_buf, 1, ctx,
	    error_out);
}

struct sbt_entry *
get_sbt_entry_randomize(int sbt_fd, btree_key_t *start_key, char **entry_buf,
    unsigned int randomization_factor, struct get_sbt_entry_ctx **ctx,
    struct isi_error **error_out)
{

	ASSERT(start_key != NULL);
	ASSERT(entry_buf != NULL);
	ASSERT(*entry_buf == NULL);
	ASSERT(ctx != NULL);
	ASSERT(*ctx == NULL || (*ctx)->magic_ == GET_ENTRY_CTX_VALID_MAGIC);
	ASSERT(randomization_factor > 0);

	struct isi_error *error = NULL;
	int result = -1;
	struct sbt_entry *ent = NULL;
	size_t entry_buf_size = 256 * randomization_factor;
	char *temp_entry_buf = (char *)malloc(entry_buf_size);
	ASSERT(temp_entry_buf != NULL);
	btree_key_t temp_key;
	size_t num_ents = 0;

	do {
		result = ifs_sbt_get_entries(sbt_fd, start_key, &temp_key,
		    entry_buf_size, temp_entry_buf, randomization_factor,
		    &num_ents);

		if (num_ents > 0) {
			/*
			 * We've retrieved one or more entries, randomly choose
			 * one of them based on the randomization_factor. (Note
			 * that a randomization_factor of '1' eliminates the
			 * randomization altogether.)
			 *
			 * Why would anyone want to do this?  Here's one use
			 * case:
			 *
			 * When starting a task, we want to randomly choose a
			 * task to avoid contention between different threads.
			 * To do so, we choose a random key between 0 and the
			 * maximum key value (e.g., 0 to 2^128).  This function
			 * will then return the first entry whose key is greater
			 * than our random key (note that we do do wrapping, so
			 * a key of value 0 is greater than a key of value
			 * 2^128).  If our keys are evenly distributed in the
			 * range of 0 - 2^128, this works great, but consider
			 * the surprisingly common case where we have many keys
			 * grouped closely together.  E.g.:
			 *
			 *   Assume we have entries with keys 100, 101, ... 200
			 *   The odds of the caller's random key falling in
			 *     that range are very, very slim (100/(2^128)).
			 *   In essence, that means nearly every thread is
			 *     trying to find the first key greater than 200
			 *     which will always be 100 (again, because of
			 *     wrapping)
			 *   So, randomization at the caller level bought
			 *     nothing with closely grouped keys.  All threads
			 *     are still in contention for the same key.
			 *
			 * To resolve the above problem, we add randomization at
			 * this level to provide a way of choosing one of the
			 * first n-keys instead.
			 */
			if (num_ents < randomization_factor)
				randomization_factor = num_ents;

			int random_index = get_random_int_between(0,
			    randomization_factor - 1);

			char *eb_iter = temp_entry_buf;

			/*
			 * Walk through the sbt_entries until we find the
			 * random_index-th entry
			 */
			for (int i = 0; i < random_index; ++i) {
				eb_iter += sizeof(struct sbt_entry) +
				    ((struct sbt_entry*)eb_iter)->size_out;
			}

			struct sbt_entry *cur_entry =
			    (struct sbt_entry*)(eb_iter);

			ASSERT(cur_entry != NULL);

			/* Copy the entry */
			ent = (struct sbt_entry *)malloc(
			    sizeof(struct sbt_entry) + cur_entry->size_out);
			ASSERT(ent != NULL);
			memcpy(ent, cur_entry, sizeof(struct sbt_entry) +
			    cur_entry->size_out);

			if (*ctx == NULL)
				get_sbt_entry_ctx_init(ctx);

			if ((*ctx)->first_key_ == NULL) {
				/*
				 * Note the key of the first retrieved entry.
				 * If this entry (or one greater, since this
				 * entry might be removed in the meantime) is
				 * retrieved again after we've hit the largest
				 * entry in the SBT, give up.  Yes, it's
				 * possible that an entry with key greater than
				 * first_key was added after the iterator
				 * passed its point in the tree - in that case,
				 * another thread (or maybe this one) will see
				 * it later - and the alternative is an
				 * infinite loop.
				 */
				(*ctx)->first_key_ = (btree_key_t *)
				    malloc(sizeof (btree_key_t));
				ASSERT((*ctx)->first_key_ != NULL);
				*((*ctx)->first_key_) = ent->key;
			}
			else if ((*ctx)->looped_) {
				/*
				 * We've passed the last entry in the SBT, so
				 * check to see if we've come back (or passed)
				 * the first key we saw.
				 */
				btree_key_t *first_key = (*ctx)->first_key_;

				if (ent->key.keys[0] > first_key->keys[0] ||
				    (ent->key.keys[0] == first_key->keys[0] &&
				     ent->key.keys[1] >= first_key->keys[1])) {
					/* We've passed our starting point (see
					 * above comment), so stop looking.
					 */
					free(ent);
					ent = NULL;

					break;
				}
			}

			*start_key = temp_key;
		}
		else {
			if (result != 0) {
				if (errno == ENOSPC) {
					/*
					 * The buffer is too small to hold the
					 * entry.  Rare, but not impossible.
					 */
					free(temp_entry_buf);

					entry_buf_size *= 2;
					temp_entry_buf =
					    (char *)malloc(entry_buf_size);
					ASSERT(temp_entry_buf != NULL);
				}
				else {
					error = isi_system_error_new(errno,
					    "unanticipated error "
					    "retrieving entry from SBT");

					break;
				}
			}
			else if (start_key->keys[0] == 0 &&
			    start_key->keys[1] == 0)
				/*
				 * The syscall was successful, but no entries
				 * were found when looking from the start of
				 * the tree, i.e. empty.
				 */
				break;
			else {
				/*
				 * The syscall was successful, but no entries
				 * were found from where start_key is to the
				 * end, so try wrapping around to the start of
				 * the tree and looking again.  If we've
				 * already seen an entry in this tree (which,
				 * if the context variable is non-NULL, we
				 * have), note that we've reached the end.
				 */
				if (*ctx != NULL)
					(*ctx)->looped_ = true;

				start_key->keys[0] = 0;
				start_key->keys[1] = 0;
			}
		}
	} while (ent == NULL);

	if (temp_entry_buf != NULL)
		free(temp_entry_buf);

	*entry_buf = (char*)ent;

	isi_error_handle(error, error_out);

	return ent;
}

static void
initialize_node_id(struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	if (!s_handling_node_id_initialized) {
		scoped_lock sl(s_handling_node_id_init_mutex);
		if (!s_handling_node_id_initialized) {
			IfsConfig ifsc;
			arr_config_load_as_ifsconf(&ifsc, &error);
			if (error)
				goto out;

			memcpy(s_handling_node_id, ifsc.guid,
			    IFSCONFIG_GUID_SIZE);
			s_handling_node_id_initialized = true;

			ifsConfFree(&ifsc);
		}
	}

 out:
	isi_error_handle(error, error_out);
}

const uint8_t *
get_handling_node_id(void)
{
	struct isi_error *error = NULL;
	uint8_t *ret = NULL;

	initialize_node_id(&error);
	if (error)
		isi_error_free(error);
	else
		ret = s_handling_node_id;

	return ret;
}

void
get_node_id_copy(uint8_t *node_id_copy)
{
	ASSERT(node_id_copy != NULL);

	struct isi_error *error = NULL;

	node_id_copy[0] = '\0';
	initialize_node_id(&error);
	if (error == NULL)
		memcpy(node_id_copy, s_handling_node_id, IFSCONFIG_GUID_SIZE);
}

static void
initialize_node_devid(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct arr_config *cfg = NULL;

	if (s_node_devid == -1) {
		scoped_lock sl(s_node_devid_init_mutex);
		if (s_node_devid == -1) {
			cfg = arr_config_load(&error);
			if (error) {
				isi_error_add_context(error,
				    "failed to initialize node devid");
				goto out;
			}

			s_node_devid = arr_config_get_local_devid(cfg);
			s_node_lnn = arr_config_get_lnn(cfg, s_node_devid);
		}
	}

 out:
	if (cfg) {
		 arr_config_free(cfg);
		 cfg = NULL;
	 }


	isi_error_handle(error, error_out);
}

int
get_devid(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int devid = -1;

	initialize_node_devid(&error);
	if (error != NULL)
		goto out;

	devid = s_node_devid;

 out:
	isi_error_handle(error, error_out);

	return devid;
}

int
get_lnn(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int lnn = -1;

	initialize_node_devid(&error);
	if (error != NULL)
		goto out;

	lnn = s_node_lnn;

 out:
	isi_error_handle(error, error_out);

	return lnn;
}

char *
get_pending_sbt_name(task_type type, int priority)
{
	struct fmt FMT_INIT_CLEAN(fmt);
	fmt_print(&fmt, "isi_cpool_d.%{}.pending.%d",
	    task_type_fmt(type), priority);
	return fmt_detach(&fmt);
}

char *
get_inprogress_sbt_name(task_type type)
{
	struct fmt FMT_INIT_CLEAN(fmt);
	fmt_print(&fmt, "isi_cpool_d.%{}.inprogress",
	    task_type_fmt(type));
	return fmt_detach(&fmt);
}

char *
get_lock_file_name(task_type type)
{
	struct fmt FMT_INIT_CLEAN(fmt);
	fmt_print(&fmt, "%sisi_cpool_d.%{}.lock",
	    CPOOL_LOCKS_DIRECTORY, task_type_fmt(type));
	return fmt_detach(&fmt);
}

char *
get_move_lock_file_name(task_type type)
{
	struct fmt FMT_INIT_CLEAN(fmt);
	fmt_print(&fmt, "%sisi_cpool_d.%{}.move_lock",
	    CPOOL_LOCKS_DIRECTORY, task_type_fmt(type));
	return fmt_detach(&fmt);
}

const char *
get_coi_sbt_name(void)
{
	static char coi_sbt_name[] = "isi_cpool_d.coi";

	return coi_sbt_name;
}

const char *
get_cmoi_sbt_name(void)
{
	static char cmoi_sbt_name[] = "isi_cpool_d.cmoi";

	return cmoi_sbt_name;
}

const char *
get_restore_coi_sbt_name(void)
{
	static char restore_coi_sbt_name[] = "isi_cpool_d.restore_coi";

	return restore_coi_sbt_name;
}

const char *
get_ooi_sbt_name(void)
{
	static char ooi_sbt_name[] = "isi_cpool_d.ooi";

	return ooi_sbt_name;
}

const char *
get_ooi_lock_filename(void)
{
	static char ooi_lock_filename[] =
		CPOOL_LOCKS_DIRECTORY "isi_cpool_d.ooi.lock";

	return ooi_lock_filename;
}

const char *
get_csi_sbt_name(void)
{
	static char csi_sbt_name[] = "isi_cpool_d.csi";

	return csi_sbt_name;
}

const char *
get_csi_lock_filename(void)
{
	static char csi_lock_filename[] =
		CPOOL_LOCKS_DIRECTORY "isi_cpool_d.csi.lock";

	return csi_lock_filename;
}

const char *
get_wbi_sbt_name(void)
{
	static char wbi_sbt_name[] = "isi_cpool_d.wbi";

	return wbi_sbt_name;
}

const char *
get_wbi_lock_filename(void)
{
	static char wbi_lock_filename[] =
		CPOOL_LOCKS_DIRECTORY "isi_cpool_d.wbi.lock";

	return wbi_lock_filename;
}

void
delete_sbt(const char *sbt_name, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int root_fd = -1, parent_fd = -1, result;

	root_fd = sbt_open_root_dir(O_WRONLY);
	if (root_fd < 0) {
		error = isi_system_error_new(errno, "failed to open SBT root");
		goto out;
	}

	parent_fd = enc_openat(root_fd, SBT_DIR, ENC_DEFAULT, O_RDONLY);
	if (parent_fd < 0) {
		if (errno != ENOENT)
			error = isi_system_error_new(errno,
			    "failed to open existing SBT dir (%s)", SBT_DIR);

		// in the case where errno == ENOENT, no parent directory
		// means no SBT to delete, so we're done either way
		goto out;
	}

	result = enc_unlinkat(parent_fd, sbt_name, ENC_DEFAULT, 0);
	if (result != 0) {
		if (errno != ENOENT)
			error = isi_system_error_new(errno,
			    "failed to unlink SBT (%s/%s)", SBT_DIR, sbt_name);
		goto out;
	}

out:
	if (root_fd >= 0)
		close(root_fd);

	if (parent_fd >= 0)
		close(parent_fd);

	isi_error_handle(error, error_out);
}

int
open_sbt(const char *name, bool create, struct isi_error **error_out)
{
	ASSERT(name != NULL);

	ilog(IL_DEBUG, "opening SBT -- name: %s create: %s",
	    name, create ? "yes" : "no");

	struct isi_error *error = NULL;
	int root_fd = -1, parent_fd = -1, res = 0, sbt_fd = -1;

	// open SBT root
	root_fd = sbt_open_root_dir(O_WRONLY);
	if (root_fd < 0) {
		error = isi_system_error_new(errno,
		    "failed to open SBT root");
		goto out;
	}

	parent_fd = enc_openat(root_fd, SBT_DIR, ENC_DEFAULT, O_RDONLY);
	if (parent_fd < 0 && errno == ENOENT) {
		res = enc_mkdirat(root_fd, SBT_DIR, ENC_DEFAULT, 0755);
		if (res == -1 && errno != EEXIST) {
			error = isi_system_error_new(errno,
			    "failed to create parent SBT directory "
			    "(%s)", SBT_DIR);
			goto out;
		}
		parent_fd = enc_openat(root_fd, SBT_DIR,
		    ENC_DEFAULT, O_RDONLY);
	}
	if (parent_fd < 0) {
		error = isi_system_error_new(errno,
		    "failed to open parent SBT directory (%s)",
		    SBT_DIR);
		goto out;
	}

	sbt_fd = enc_openat(parent_fd, name, ENC_DEFAULT, O_RDWR);
	if (sbt_fd < 0 && errno == ENOENT) {
		if (create) {
			// create then open
			res = ifs_sbt_create(parent_fd, name,
			    ENC_DEFAULT, SBT_128BIT);
			if (res == -1 && errno != EEXIST) {
				error = isi_system_error_new(errno,
				    "failed to create SBT (%s/%s)",
				    SBT_DIR, name);
				goto out;
			}
			sbt_fd = enc_openat(parent_fd, name, ENC_DEFAULT,
			    O_RDWR);
		}
		else {
			error = isi_system_error_new(ENOENT,
			    "SBT (%s/%s) does not exist", SBT_DIR, name);
			goto out;
		}
	}
	if (sbt_fd < 0) {
		error = isi_system_error_new(errno,
		    "failed to open SBT file %s/%s",
		    SBT_DIR, name);
		goto out;
	}

	ilog(IL_DEBUG, "opened SBT -- name: %s fd: %d", name, sbt_fd);

 out:
	if (parent_fd >= 0)
		close(parent_fd);

	if (root_fd >= 0)
		close(root_fd);

	isi_error_handle(error, error_out);

	return (sbt_fd < 0 ? -1 : sbt_fd);
}

void
close_sbt(int sbt_fd)
{
	ASSERT(sbt_fd >= 0);

	int res = close(sbt_fd);
	if (res != 0)
		ilog(IL_NOTICE, "failed to close SBT (fd: %d): %s",
		    sbt_fd, strerror(errno));
}

// off_t is a signed variable this means that only
// 63 bits are used. Handing negative value to the
// ifs_cpool_flock would fail. 
// This routine fixes the issue as long as the 
// length is one byte.
//
static void
fix_key_value(off_t &key)
{
	if (key < 0) 
		key = -key;
}
void
populate_flock_params(off_t key, struct flock *params)
{
	fix_key_value(key);
	params->l_start = key;
	params->l_len = 1;
	params->l_pid = getpid();
	params->l_type = CPOOL_LK_X;
	params->l_whence = SEEK_SET;
	params->l_sysid = 0;
}

int
cpool_dom_lock_ex(int lock_fd, off_t key)
{
	struct flock params;

        populate_flock_params(key, &params);

	return ifs_cpool_flock(lock_fd, LOCK_DOMAIN_CPOOL_JOBS,
	    F_SETLK, &params, F_SETLKW);
}

int
cpool_dom_unlock(int lock_fd, off_t key)
{
	struct flock params;

        populate_flock_params(key, &params);

	return ifs_cpool_flock(lock_fd, LOCK_DOMAIN_CPOOL_JOBS,
	    F_UNLCK, &params, 0);
}

char *
get_daemon_jobs_sbt_name(void)
{
	const int ret_buf_size = 128;
	char *ret_buf = (char *)malloc(ret_buf_size);
	ASSERT(ret_buf != NULL);

	int correct_buf_size = snprintf(ret_buf, ret_buf_size,
	    "isi_cpool_d.%s", CPOOL_DAEMON_JOBS_FILE);

	// make sure we got the whole string
	ASSERT(correct_buf_size < ret_buf_size);

	return ret_buf;
}

char *
get_djob_record_lock_file_name(void)
	{
	const int ret_buf_size = 128;
	char *ret_buf = (char *)malloc(ret_buf_size);
	ASSERT(ret_buf != NULL);

	int correct_buf_size = snprintf(ret_buf, ret_buf_size,
	    CPOOL_LOCKS_DIRECTORY "isi_cpool_d.%s",
	    CPOOL_DJOB_RECORD_LOCK_FILE);

	// make sure we got the whole string
	ASSERT(correct_buf_size < ret_buf_size);

	return ret_buf;
}

char *
get_djob_file_list_sbt_name(djob_id_t daemon_job_id)
{
	const int ret_buf_size = 128;
	char *ret_buf = (char *)malloc(ret_buf_size);
	ASSERT(ret_buf != NULL);

	int correct_buf_size = snprintf(ret_buf, ret_buf_size,
	    "isi_cpool_d.%s.%u", CPOOL_DAEMON_JOB_FILE_LIST,
	    daemon_job_id);

	// make sure we got the whole string
	ASSERT(correct_buf_size < ret_buf_size);
	return ret_buf;
}

void
abbreviate_path(const char *path_in, char *path_out, size_t out_size)
{
	ASSERT(path_in && path_out);
	size_t path_sz = strlen(path_in);

	// No need to abbreviate, we're already short enough
	if (out_size > path_sz) {
		strcpy(path_out, path_in);
		return;
	}

	// Remove some characters from the middle and replace with
	//    ellipsis
	const int leading_characters = (out_size > 30 ? 15 : 0);
	const char conjunction[] = "...";
	const int non_trailing_characters = leading_characters +
	    strlen(conjunction);
	const int trailing_characters = out_size - non_trailing_characters - 1;
	unsigned int beginning_of_the_end = path_sz - trailing_characters;

	// Copy the beginning of the string
	strncpy(path_out, path_in, leading_characters);

	// Append the ellipsis
	strcpy(path_out + leading_characters, conjunction);

	// UTF-8 characters are variable byte length.  We need to make sure we
	//  don't break in the middle of a UTF-8 character
	for (int i = 0;
	    i < 4 && !IS_START_OF_UTF8_CHAR(path_in[beginning_of_the_end]);
	    i++) {
		beginning_of_the_end++;
		if (beginning_of_the_end >= path_sz) {
			return;
		}
	}
	// Copy the end of the string
	strcpy(path_out + non_trailing_characters,
	    &path_in[beginning_of_the_end]);
}

/*
 * Check if the node has network connectivity.
 * Check the network interface of the node.
 */
void check_network_connectivity(bool *network_up, struct isi_error **error_out)
{
	ASSERT(network_up != NULL);

	struct isi_error *error = NULL;

	ifs_devid_t devid;
	struct flx_config *fnc = NULL;
	struct flx_node_info const *node = NULL;
	struct lni_list *lni_list = NULL;
	struct lni_list_iter *lni_iter = NULL;
	struct lni_info *lni = NULL;
	bool result = false;

	devid = get_devid(&error);
	ON_ISI_ERROR_GOTO(out, error);

	fnc = flx_config_open_read_only(NULL, &error);
	ON_ISI_ERROR_GOTO(out, error);

	node = flx_config_find_node(fnc, devid);
	if (node == NULL) {
		error = isi_system_error_new(ENOENT,
		    "flx_config_find_node() returned NULL");
		goto out;
	}

	lni_list = flx_node_info_get_lni_list(node);
	if (lni_list == NULL) {
		error = isi_system_error_new(ENOENT,
		    "flx_node_info_get_lni_list() returned NULL");
		goto out;
	}

	lni_iter = lni_list_iterator(lni_list);
	if (lni_iter == NULL) {
		error = isi_system_error_new(ENOENT,
		    "lni_list_iterator() returned NULL");
		goto out;
	}

	while (lni_list_has_next(lni_iter)) {
		lni = lni_list_next(lni_iter);
		if (nic_get_stat(lni_get_nic(lni)) == NIC_STAT_UP) {
			result = true;
			goto out;
		}
	}

 out:
	if (fnc) {
		flx_config_free(fnc);
		fnc = NULL;
	}

	if (lni_iter) {
		lni_list_iter_free(lni_iter);
		lni_iter = NULL;
	}

	if (error == NULL)
		*network_up = result;

	isi_error_handle(error, error_out);
}

int open_djob_file_list_sbt(djob_id_t job_id, bool create,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int sbt_fd = -1;
	char *djob_file_list_name = get_djob_file_list_sbt_name(job_id);

	ASSERT(djob_file_list_name);

	sbt_fd = open_sbt(djob_file_list_name, create, &error);

	free(djob_file_list_name);

	isi_error_handle(error, error_out);
	return sbt_fd;
}

const char *
lookup_policy_name(uint32_t policy_id, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	const char * policy_name = NULL;
	struct smartpools_context *context = NULL;
	struct fp_policy *policy = NULL;

	if (CPOOL_INVALID_POLICY_ID == policy_id) {
		error = isi_system_error_new(EINVAL,
		    "passed-in policy_id %d is invalid", policy_id);
		goto out;
	}

	smartpools_open_context(&context, &error);
	ON_ISI_ERROR_GOTO(out, error);

	smartpools_get_policy_by_id(policy_id, &policy, context, &error);
	ON_ISI_ERROR_GOTO(out, error);

	ASSERT(NULL != policy);
	policy_name = strdup(policy->name);
	if (NULL == policy_name){
		error = isi_system_error_new(EINVAL,
		    "returned policy name is NULL");
		goto out;
	}

 out:
	if (NULL != context)
		smartpools_close_context(&context,
		    isi_error_suppress(IL_NOTICE));

	isi_error_handle(error, error_out);

	return policy_name;
}

int
determine_archive_task_version(void)
{
	int version = CPOOL_ARCHIVE_TASK_V1;
	FILE *fp = NULL;

	fp = fopen(CPOOL_ARCHIVE_TASK_VERSION_FLAG_FILE,"r");

	//go back to archive task v1 since we cannot get the flag file
	if (NULL == fp) {
		ilog(IL_ERR,
		    "fallback to archive task v1 due to fail to open flag file %s, errno: %d, error: %s",
		    CPOOL_ARCHIVE_TASK_VERSION_FLAG_FILE,
		    errno,
		    strerror(errno));
		version = CPOOL_ARCHIVE_TASK_V1;
	} else {
		version = CPOOL_ARCHIVE_TASK_VERSION;

		fclose(fp);
		fp = NULL;
	}

	return version;
}
