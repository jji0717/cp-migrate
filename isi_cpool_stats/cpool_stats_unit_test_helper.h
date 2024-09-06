#ifndef __ISI_CPOOL_STATS_UNIT_TEST_HELPER_H__
#define __ISI_CPOOL_STATS_UNIT_TEST_HELPER_H__

#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include "cpool_stat_manager.h"

class cpool_stats_unit_test_helper {
public:
	static void on_suite_setup()
	{
		struct isi_error *error = NULL;

		cpool_stat_manager *mgr =
		    cpool_stat_manager::get_new_instance(&error);
		fail_if(error != NULL,
		    "Error getting cpool_stat_manager instance: %#{}",
		    isi_error_fmt(error));
		fail_if(mgr == NULL,
		    "Received NULL cpool_stat_manager instance");

		/*
		 * N.b., hardcoded value below could be passed in, but that
		 * seems like overkill at this point.  We'll just make the
		 * assumption that no more than 20 accounts will be needed for
		 * any given unit test
		 */
		mgr->preallocate_space_for_tests(20, &error);

		delete mgr;
	};
};

#endif // __ISI_CPOOL_STATS_UNIT_TEST_HELPER_H__
