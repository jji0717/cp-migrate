#ifndef __ECS_IO_HELPER_H__
#define __ECS_IO_HELPER_H__

/**
 * @file
 *
 * ECS IO helper class for Cloud IO for CBM layer
 *
 * Copyright (c) 2012-2015
 * EMC Isilon Systems, Inc.  All rights reserved.
 *
 */

#include "s3_io_helper.h"

class ecs_io_helper : public s3_io_helper {
	public:
		explicit ecs_io_helper(const isi_cbm_account &acct);
};


#endif //__ECS_IO_HELPER_H__
