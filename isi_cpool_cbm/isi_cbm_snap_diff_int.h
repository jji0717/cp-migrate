#ifndef _ISI_CBM_SNAP_DIFF_INT_H_
#define _ISI_CBM_SNAP_DIFF_INT_H_

/**
 * Internal types and details
 */

#include "isi_cbm_snap_diff.h"

enum isi_cbm_file_region_type
{
	FRT_INVALID 	= -1,	/* file region is invalid */
	FRT_UNCACHED	= 1,  	/* file region is not cached */
	FRT_CACHE_READ	= 2,  	/* file region has been cache-read */
	FRT_CACHE_WRITE = 4, 	/* file region has been cache-modified */
	FRT_ORDINARY = 8	/* file region for non-stub file */
};

struct isi_cbm_file_region
{
	isi_cbm_file_region(): frt(FRT_INVALID), offset(0), length(0) {}
	isi_cbm_file_region(enum isi_cbm_file_region_type _frt, off_t _offset, 
	    size_t _length): frt(_frt), offset(_offset),  length(_length) {}


	// operator== XXX

	
	enum isi_cbm_file_region_type frt;
	off_t offset;
	size_t length;
};

#endif // _ISI_CBM_SNAP_DIFF_INT_H_
