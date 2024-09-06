#ifndef __RAN_IO_HELPER_H__
#define __RAN_IO_HELPER_H__

/**
 * @file
 *
 * RAN IO helper class for Cloud IO for CBM layer
 *
 * Copyright (c) 2012-2013
 * EMC Isilon Systems, Inc.  All rights reserved.
 *
 */

#include "isi_cbm_ioh_base.h"

class ran_io_helper : public isi_cbm_ioh_base {

public:
	/** Constructor */
	ran_io_helper(const isi_cbm_account &acct); //////account提供这个ran的cdo_container, cmo_container,.....

	/** Destructor */
	virtual ~ran_io_helper() {};

	/** Get cloud provider */
	virtual isi_cloud::cl_provider *get_provider() { return ran_p_; }

	/**
	 * Build the full container path from the base container
	 * name and the object id
	 * Return value is a count of the number of levels added after
	 * the container name
	 * @param containder[in] - RAN directory name
	 * @param object_id[in]  - RAN object or file
	 * @param container_path[out] - The full container path
	 */
	virtual int build_container_path(  ////创建cloud上完整的cdo/cmo目录
	    const std::string &container,////每一个account的cdo/cmo的base container
	    const std::string &object_id,
	    std::string &container_path);

private:
	/**
	 * Routine to build the directory hash string from the object_id
	 * @param object_id[in]  - RAN object or file
	 * @param hash_str[out] - The hash string
	 */
	void
	hash_object_id(
	    const std::string &object_id,
	    std::string &hash_str);

	isi_cloud::cl_provider *ran_p_;
};


#endif //__RAN_IO_HELPER_H__
