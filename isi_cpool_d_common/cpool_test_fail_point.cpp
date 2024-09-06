#include <isi_util/checker.h>

#include "cpool_test_fail_point.h"

cpool_test_fail_point::cpool_test_fail_point(const char *name)
{
	ufp_globals_wr_lock();
	ufp_ = ufail_point_lookup(name);
	fail_if(!ufp_, "failed to find %s failpoint", name);
	ufp_globals_wr_unlock();
}

cpool_test_fail_point::~cpool_test_fail_point()
{
	if (ufp_) {
		ufp_globals_wr_lock();
		fail_if(ufail_point_set(ufp_, "off"));
		ufp_globals_wr_unlock();
	}
}

void
cpool_test_fail_point::set(const char *value)
{
	if (ufp_) {
		ufp_globals_wr_lock();
		fail_if(ufail_point_set(ufp_, value));
		ufp_globals_wr_unlock();
	}
}
