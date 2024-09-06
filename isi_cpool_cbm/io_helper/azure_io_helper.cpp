/**
 * @file
 *
 * Azure IO helper class for Cloud IO for CBM layer
 *
 * Copyright (c) 2012-2013
 * EMC Isilon Systems, Inc.  All rights reserved.
 *
 */

#include "azure_io_helper.h"
#include "compressed_istream.h"
#include "encrypted_istream.h"
#include "headered_istream.h"
#include "header_ostream.h"
#include "checksum_istream.h"
#include "isi_cbm_ioh_creator.h"
#include <isi_cpool_stats/isi_cpool_stats_api_cb.h>

using namespace isi_cloud;

namespace {

/**
 * Routine to create an instance of azure io helper
 * @param acct[in] azure account
 * @return pointer to the created instance
 */
isi_cbm_ioh_base *
create_azure_io_helper(const isi_cbm_account &acct)
{
	return new azure_io_helper(acct);
}

/**
 * Register object that will register creation function to io helper
 * factory upon module loading
 */
isi_cbm_ioh_register g_reg(CPT_AZURE, create_azure_io_helper);

}

azure_io_helper::azure_io_helper(const isi_cbm_account &acct)
	: isi_cbm_ioh_base(acct)
{
	ASSERT(acct.type == CPT_AZURE,
	    "Only Azure should use azure_io_helper (%d requested)", acct.type);
	azure_p_ = &cl_provider_creator::get_provider(CPT_AZURE);

	// no need to free ran provider pointer
	// azure_p_ = dynamic_cast<azure_provider *>(&prod);
	// ASSERT(azure_p_);
}
