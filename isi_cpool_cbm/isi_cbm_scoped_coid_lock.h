#pragma once

#include "isi_cbm_coi.h"

using namespace isi_cloud;

/**
 * scoped cloud object ID locker
 */
class scoped_coid_lock {
public:
	scoped_coid_lock(coi &coi,
	    const isi_cloud_object_id &obj_id) :
	    coi_(coi), locked_(false), coid_(obj_id) {}

	~scoped_coid_lock();

	/**
	 * Lock the file for write back
	 */
	void lock(bool wait, struct isi_error **error_out);
private:
	coi &coi_;
	bool locked_;
	isi_cloud_object_id coid_;
	void unlock();

};
