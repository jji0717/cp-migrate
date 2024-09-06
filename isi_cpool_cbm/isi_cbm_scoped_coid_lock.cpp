#include "isi_cbm_scoped_coid_lock.h"

#include <isi_util/isi_error.h>
#include <isi_util_cpp/isi_exception.h>

scoped_coid_lock::~scoped_coid_lock()
{
	if(locked_) {

		coi_.unlock_coid(coid_, isi_error_suppress());
		locked_ = false;
	}
}

void
scoped_coid_lock::lock(bool wait, struct isi_error **error_out)
{
	ASSERT(!locked_);

	coi_.lock_coid(coid_, wait, error_out);
	if (*error_out == NULL) {
		locked_ = true;
	}
}

