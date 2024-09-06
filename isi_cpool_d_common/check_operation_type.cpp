#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>
#include <ifs/ifs_syscalls.h>
#include <ifs/ifs_types.h>

#include "operation_type.h"

SUITE_DEFINE_FOR_FILE(check_operation_type,
    .mem_check = CK_MEM_LEAKS,
    .timeout = 0,
    .fixture_timeout = 1200);

TEST(test_serialize_deserialize)
{
	const cpool_operation_type *op_type_from = OT_ARCHIVE;
	const cpool_operation_type *op_type_to = OT_NONE;

	size_t serialized_size = op_type_from->get_packed_size();
	void *serialized = malloc(serialized_size);
	fail_if (serialized == NULL);

	void *temp = serialized;
	op_type_from->pack(&temp);

	temp = serialized;
	op_type_to = op_type_to->unpack(&temp);

	fail_if (op_type_to != OT_ARCHIVE,
	    "(exp: %p act: %p)", OT_ARCHIVE, op_type_to);

	free(serialized);
}
