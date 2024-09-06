#ifndef __S3_IO_HELPER_H__
#define __S3_IO_HELPER_H__

/**
 * @file
 *
 * S3 IO helper class for Cloud IO for CBM layer
 *
 * Copyright (c) 2012-2013
 * EMC Isilon Systems, Inc.  All rights reserved.
 *
 */

#include "isi_cbm_ioh_base.h"

class s3_io_helper : public isi_cbm_ioh_base {

 public:
	/** Constructor */
	s3_io_helper(const isi_cbm_account &acct);

	/** Destructor */
	virtual ~s3_io_helper();

	/** Get cloud provider */
	virtual isi_cloud::cl_provider *get_provider() { return s3_p_; }

 protected:

	virtual void match_cmo_attr(const str_map_t *cmo_attr_to_match,
	    const cl_object_name &cloud_name);

 private:
	isi_cloud::cl_provider *s3_p_;
};


#endif //__S3_IO_HELPER_H__
