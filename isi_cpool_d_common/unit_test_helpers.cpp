#include <ifs/ifs_syscalls.h>
#include <ifs/ifs_types.h>
#include <isi_sbtree/sbtree.h>
#include <isi_ufp/isi_ufp.h>
#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include "cpool_d_debug.h"
#include "hsbt.h"
#include "operation_type.h"
#include "task_key.h"

#include "unit_test_helpers.h"

void
add_sbt_entry_ut(int sbt_fd, const task_key *key, const void *entry_buf,
    size_t entry_len)
{
	fail_if (sbt_fd <= 0, "sbt_fd: %d", sbt_fd);

	fail_if(key == NULL);

	fail_if(entry_buf == NULL);
	fail_if(entry_len <= 0, "entry_len: %zu", entry_len);

	const void *serialized_key;
	size_t serialized_key_size;
	key->get_serialized_key(serialized_key, serialized_key_size);

	if (key->get_op_type()->uses_hsbt()) {
		struct isi_error *error = NULL;
		hsbt_add_entry(sbt_fd, serialized_key, serialized_key_size,
		    entry_buf, entry_len, NULL, &error);
		fail_if(error != NULL, "failed to add entry to hsbt: %#{}",
		    isi_error_fmt(error));
	}
	else {
		fail_if(serialized_key_size != sizeof(btree_key_t),
		    "%zu != %zu", serialized_key_size, sizeof(btree_key_t));

		const btree_key_t *bt_key =
		    static_cast<const btree_key_t *>(serialized_key);
		fail_if(bt_key == NULL);

		int result = ifs_sbt_add_entry(sbt_fd,
		    const_cast<btree_key_t *>(bt_key), entry_buf, entry_len,
		    NULL);
		fail_if(result != 0, "failed to add entry to sbt: %s",
		    strerror(errno));
	}
}

void
get_sbt_entry_ut(int sbt_fd, const task_key *key, void *entry_buf,
    size_t entry_size, size_t *entry_size_out, bool ignore_ENOENT)
{
	fail_if (sbt_fd <= 0, "sbt_fd: %d", sbt_fd);
	fail_if (key == NULL);
	// entry_size_out can be NULL

	const void *serialized_key;
	size_t serialized_key_size;
	key->get_serialized_key(serialized_key, serialized_key_size);

	if (key->get_op_type()->uses_hsbt()) {
		size_t local_entry_size_out = entry_size;
		size_t *entry_size_out_to_use = &local_entry_size_out;

		if (entry_size_out != NULL) {
			*entry_size_out = entry_size;
			entry_size_out_to_use = entry_size_out;
		}

		struct isi_error *error = NULL;
		hsbt_get_entry(sbt_fd, serialized_key, serialized_key_size,
		    entry_buf, entry_size_out_to_use, NULL, &error);
		if (error != NULL) {
			if (!ignore_ENOENT ||
			    !isi_system_error_is_a(error, ENOENT))
				fail ("failed to get entry from hsbt: %#{}",
				    isi_error_fmt(error));
			isi_error_free(error);
		}
	}
	else {
		fail_if(serialized_key_size != sizeof(btree_key_t),
		    "%zu != %zu", serialized_key_size, sizeof(btree_key_t));

		const btree_key_t *bt_key =
		    static_cast<const btree_key_t *>(serialized_key);
		fail_if(bt_key == NULL);

		int result = ifs_sbt_get_entry_at(sbt_fd,
		    const_cast<btree_key_t *>(bt_key), entry_buf, entry_size,
		    NULL, entry_size_out);
		if (result != 0) {
			if (!ignore_ENOENT || errno != ENOENT)
				fail ("failed to get entry from sbt: %s",
				    strerror(errno));
		}
	}
}

void
init_failpoints_ut(void)
{
	static bool initialized = false;
	if (!initialized) {
		fail_unless(UFAIL_POINT_INIT("isi_check") == 0);
		initialized = true;
	}
}

/*
 * Running isi_cpool_d asynchronously in the backgroun can lead to conflicts
 * with unit tests (e.g., when isi_cpool_d attempts to process tasks that
 * are being tested.  To avoid issues, we can disable isi_cpool_d before test
 * execution and reenable after. isi_for_array is used to avoid having to wait
 * for the disable to propagate to all nodes before starting tests.
 *
 * Note that if a test core dumps and fails to reenable isi_cpool_d itself, we
 * can reenable at the CLI via 'isi services isi_cpool_d enable'
 *
 * Also note that this function *could* be done using APIs instead of a system
 * call (e.g., mcp_client_set_enable(...)), however, calling into mcp's APIs
 * invokes likewise which (somehow) prevents test processes from exiting.
 * There is probably a way to make this happen cleanly, but given that the scope
 * of the change is centered around a unit test, the effort is not justified at
 * this point.
 */
void
enable_cpool_d(bool enable)
{
	if (enable) {
		system("isi services isi_cpool_d enable > /dev/null");
	}
	else {
		system("isi_for_array 'isi services isi_cpool_d disable' > /dev/null");
		system("isi_for_array 'killall -9 isi_cpool_d' &> /dev/null");
	}
}
