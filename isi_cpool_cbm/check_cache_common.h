#ifndef __CHECK_CACHE_COMMON__H__
#define __CHECK_CACHE_COMMON__H__

struct cache_status_entry {
	off_t start;
	off_t end;
	enum isi_cbm_cache_status status;
};

struct cache_verify {
	struct cpool_cache_header header;
	struct cache_status_entry *entries;
	int num_entries;
};

#define DLR	DEFAULT_LAST_REGION
#define DFSZ	FILESIZE
#define FSZ(r)	((r) * REGIONSIZE)
#define ICNC	ISI_CPOOL_CACHE_NOTCACHED
#define ICC	ISI_CPOOL_CACHE_CACHED
#define ICCD	ISI_CPOOL_CACHE_DIRTY
#define ICIP	ISI_CPOOL_CACHE_INPROGRESS

#define VENTRY(s,e,st) \
	{(s), (e), (st)}
	
/* The file size field is no longer used in the cacheinfo
 * It has been renamed and left in place to leave on disk
 * size the same but it will always be zero.
 * This macros will now always set the verifier to zero
 * as well, no matter what sz says.
 */
#define VHEADER(lr, sz, lin)			\
{						\
	CPOOL_CBM_CACHE_DEFAULT_VERSION,	\
	ISI_CBM_CACHE_DEFAULT_FLAGS,		\
	(lin),					\
	0,					\
	REGIONSIZE,				\
	ISI_CBM_CACHE_DEFAULT_DATAOFFSET,	\
	(lr),					\
	ISI_CBM_CACHE_DEFAULT_SEQUENCE,		\
	ISI_CBM_CACHE_MAGIC			\
}

#define VERIFIER(rs, sz, lin, num)					\
	static cache_verify v##num = {					\
		VHEADER((rs),(sz),(lin)),				\
		e##num,							\
		sizeof(e##num)/sizeof(struct cache_status_entry),	\
	}

#endif /* __CHECK_CACHE_COMMON__H__ */

