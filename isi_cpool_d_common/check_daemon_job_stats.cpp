#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>
#include <ifs/ifs_syscalls.h>
#include <ifs/ifs_types.h>

#include "daemon_job_stats.h"

SUITE_DEFINE_FOR_FILE(check_daemon_job_stats,
    .mem_check = CK_MEM_LEAKS,
    .timeout = 0,
    .fixture_timeout = 1200);

class daemon_job_stats_ut : public daemon_job_stats
{
public:
	void set_num_members(int num_members)
	{
		num_members_ = num_members;
	}

	void set_num_succeeded(int num_succeeded)
	{
		num_succeeded_ = num_succeeded;
	}

	void set_num_failed(int num_failed)
	{
		num_failed_ = num_failed;
	}

	void set_num_pending(int num_pending)
	{
		num_pending_ = num_pending;
	}

	void set_num_inprogress(int num_inprogress)
	{
		num_inprogress_ = num_inprogress;
	}
};

TEST(test_serialize_deserialize)
{
	struct isi_error *error = NULL;
	daemon_job_stats_ut djob_stats_from, djob_stats_to;

	int num_members = 17;
	int num_succeeded = 14;
	int num_failed = 19;
	int num_pending = 3;
	int num_inprogress = 145;

	djob_stats_from.set_num_members(num_members);
	djob_stats_from.set_num_succeeded(num_succeeded);
	djob_stats_from.set_num_failed(num_failed);
	djob_stats_from.set_num_pending(num_pending);
	djob_stats_from.set_num_inprogress(num_inprogress);

	const void *serialized;
	size_t serialized_size;
	djob_stats_from.get_serialization(serialized, serialized_size);

	djob_stats_to.set_serialization(serialized, serialized_size, &error);
	fail_if (error != NULL, "unexpected error: %#{}",
	    isi_error_fmt(error));

	fail_if (djob_stats_to.get_num_members() != num_members,
	    "(exp: %d act: %d)", num_members, djob_stats_to.get_num_members());
	fail_if (djob_stats_to.get_num_succeeded() != num_succeeded,
	    "(exp: %d act: %d)", num_succeeded,
	    djob_stats_to.get_num_succeeded());
	fail_if (djob_stats_to.get_num_failed() != num_failed,
	    "(exp: %d act: %d)", num_failed, djob_stats_to.get_num_failed());
	fail_if (djob_stats_to.get_num_pending() != num_pending,
	    "(exp: %d act: %d)", num_pending, djob_stats_to.get_num_pending());
	fail_if (djob_stats_to.get_num_inprogress() != num_inprogress,
	    "(exp: %d act: %d)", num_inprogress,
	    djob_stats_to.get_num_inprogress());
}
