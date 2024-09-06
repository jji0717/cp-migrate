#ifndef __CPOOL_TEST_FAIL_POINT_H__
#define __CPOOL_TEST_FAIL_POINT_H__

#include <sys/types.h>
#include <sys/sysctl.h>

#include <stdint.h>

#include <isi_ufp/isi_ufp.h>
//////////////////////////////////////设置cpool的failpoint  check_gc_checkpointing.cpp中用到
class cpool_test_fail_point {
public:
	/**
	 * Instantiate the fail point.
	 * "name" should match the failpoint name as instantiated in code
	 *  (ie "sync_sub_cdo_err").
	 */
	explicit cpool_test_fail_point(const char *name);
	~cpool_test_fail_point();

	/**
	 * Set the given failpoint to the given value.
	 * "value" should be a valid fail-point setting (ie "return" or "off")
	 */
	void set(const char *value);

private:
	struct ufp *ufp_;
};

#define cbm_test_fail_point cpool_test_fail_point

#endif // __CPOOL_TEST_FAIL_POINT_H__
