#ifndef __AZURE_IO_HELPER_H__
#define __AZURE_IO_HELPER_H__

/**
 * @file
 *
 * Azure IO helper class for Cloud IO for CBM layer
 *
 * Copyright (c) 2012-2013
 * EMC Isilon Systems, Inc.  All rights reserved.
 *
 */

#include "isi_cbm_ioh_base.h"

class azure_io_helper : public isi_cbm_ioh_base {

public:
	/** Constructor */
	azure_io_helper(const isi_cbm_account &acct);

	/** Destructor */
	virtual ~azure_io_helper() {}

	/** Get cloud provider */
	virtual isi_cloud::cl_provider *get_provider() { return azure_p_; }

	isi_cloud::cl_provider *azure_p_;
};


#endif //__AZURE_IO_HELPER_H__
