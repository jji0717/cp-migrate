#ifndef __ISI_CBM_IOH_CREATOR_H__
#define __ISI_CBM_IOH_CREATOR_H__

/**
 * @file
 *
 * This defines interface for Cloud IO helper for CBM layer
 *
 * Copyright (c) 2012-2013
 * EMC Isilon Systems, Inc.  All rights reserved.
 *
 */

#include <isi_cloud_api/isi_cloud_api.h>

struct isi_cbm_account;
class isi_cbm_ioh_base;

/** io helper instance creation function prototype */
typedef isi_cbm_ioh_base *(*ioh_create_fun)(const isi_cbm_account &acct);

/** io helper factory class */
class isi_cbm_ioh_creator {
public:
	/**
	 * Return a pointer of isi_cbm_ioh_base for given provider type.
	 * The returned pointer may be NULL if the given provider type is not
	 * supported.
	 *
	 * @param type[in] the provider type
	 * @return pointer to isi_cbm_ioh_base.
	 */
	static isi_cbm_ioh_base *get_io_helper(  //////工厂模式
	    enum cl_provider_type type, const isi_cbm_account &acct);


};

/** class which helps register io helper function */
class isi_cbm_ioh_register
{
public:
	/** Contructor which will do register work */
	isi_cbm_ioh_register(enum cl_provider_type type,
	    ioh_create_fun creator);
};


#endif //__ISI_CBM_IOH_CREATOR_H__
