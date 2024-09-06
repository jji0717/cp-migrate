/**
 *  @file iobj_licensing.h 
 *
 * A module for licensing the object library
 *
 * Copyright (c) 2011
 *	Isilon Systems, LLC.  All rights reserved.
 */
#ifndef __IOBJ_LICENSING_H__
#define __IOBJ_LICENSING_H__

#include <stdbool.h>
#include <isi_licensing/licensing.h>

#define	IOBJ_LICENSE_CHECK_PERIOD	30		// seconds

#define	IOBJ_LICENSING_UNLICENSED	"Unlicensed"

#ifdef __cplusplus
extern "C" {
#endif

struct isi_error;

/**
 * Set a license ID for the object library.  
 *
 *  @param module_id  a value that sets the licensing identity of the object
 *  library. Applicable IDs start with the prefix ISI_LICENSING_OBJECT and
 *  defined, centrally, at isi_licensing/licensing.h.
 *  A special ID IOBJ_LICENSING_UNLICENSED can be set to disable licensing
 *  and ensure that license validity checks always succeeds.
 *
 * Note: The license key validity is evaluated against the license ID
 * (other than when set to IOBJ_LICENSING_UNLICENSED) before this function
 * returns.
 * 
 */
void
iobj_set_license_ID(const char *module_id);

/**
 * Check license validity
 *
 * @return true if the applicable license key is determined to be valid;
 * false otherwise
 * 
 * Note: This routine relies on periodically computed results. Hence there
 * may be a lag (<= IOBJ_LICENSE_CHECK_PERIOD) between setting a new license key
 * and the determination that it is valid (or invalid). 
 */
bool iobj_is_license_valid(void);

#ifdef __cplusplus
}
#endif

#endif /* __IOBJ_LICENSING_H__ */
