#ifndef __ISI_CBM_ERROR_H__
#define __ISI_CBM_ERROR_H__

#include <isi_util/isi_error.h>

__BEGIN_DECLS

/*
 * Developers: Add new CBM error types here as needed.  Assigning specific
 * integer values is unnecessary, but the *order* of existing types MUST NOT be
 * changed.  Furthermore, this enum is used as input to a rickety Python script
 * (isi_cbm_error_gen.py) so new error types must be added before
 * MAX_CBM_ERROR_TYPE.
 */
enum cbm_error_type_values {
	CBM_SUCCESS = 0, // must be zero and first
	CBM_STD_EXCEPTION_FAILURE, // likely programming error
	CBM_ISI_EXCEPTION_FAILURE, // likely programming error
	CBM_CLAPI_COULDNT_CONNECT, // could not connect to target
	CBM_CLAPI_CONNECT_DENIED, // connect being denied
	CBM_CLAPI_AUTHENTICATION_FAILED, // authentication failure
	CBM_CLAPI_ACCOUNT_IS_DISABLED,
	CBM_CLAPI_SERVER_BUSY,  // busy error returned from provider side
	CBM_CLAPI_SERVER_ERROR, // provider server internal error
	CBM_CLAPI_NETWORK_RW_FAILED, // network read or write error
	CBM_CLAPI_INVALID_KEY,
	CBM_CLAPI_OUT_OF_MEMORY,
	CBM_CLAPI_TIMED_OUT,
	CBM_CLAPI_ABORTED_BY_CALLBACK,
	CBM_CLAPI_CALLBACK_RW_FAILED,
	CBM_CLAPI_CONTAINER_EXISTS,
	CBM_CLAPI_CONTAINER_BEING_DELETED,
	CBM_CLAPI_CONTAINER_NOT_FOUND,
	CBM_CLAPI_OBJECT_NOT_FOUND,
	CBM_CLAPI_OBJECT_EXISTS,
	CBM_CLAPI_BAD_REQUEST, // likely programming error or server changed
	CBM_CLAPI_UNEXPECTED,  // server side setting altered,

	CBM_LIMIT_EXCEEDED,
	CBM_INTEGRITY_FAILURE,
	CBM_PERM_ERROR,
	CBM_STALE_STUB_ERROR,
	CBM_NOSYS_ERROR,
	CBM_INVALID_PARAM_ERROR,
	CBM_NOT_SUPPORTED_ERROR,
	CBM_FILE_TYPE_ERROR,
	CBM_READ_ONLY_ERROR,
	CBM_CACHE_HEADER_ERROR,
	CBM_CACHE_EXTEND_ERROR,
	CBM_CACHEINFO_ERROR,
	CBM_NO_REGION_ERROR,
	CBM_ALREADY_STUBBED,
	CBM_NOT_A_STUB,
	CBM_MAP_EMPTY,
	CBM_MAP_ENTRY_NOT_FOUND,
	CBM_CDO_WRITE_FAILURE,
	CBM_LIN_EXISTS,
	CBM_LIN_DOES_NOT_EXIST,
	CBM_NO_IO_HELPER,
	CBM_STUBMAP_MISMATCH,
	CBM_SIZE_MISMATCH,
	CBM_MAP_INFO_ERROR,
	CBM_SECT_HEAD_ERROR,
	CBM_SYNC_REC_ERROR,
	CBM_MOD_REC_ERROR,
	CBM_INVALID_ACCOUNT,
	CBM_INVALID_CACHE,
	CBM_NO_MAP_ENTRY,
	CBM_FILE_PAUSED,
	CBM_FILE_CANCELLED,
	CBM_FILE_STOPPED,
	CBM_DOMAIN_LOCK_FAILURE,
	CBM_DOMAIN_LOCK_CONTENTION,
	CBM_INVALIDATE_FILE_DIRTY, // tried to invalidate file with dirty region
	CBM_CHECKSUM_ERROR,
	CBM_CACHE_NO_HEADER,       // the cache header has not been written yet
	CBM_CACHE_VERSION_MISMATCH,// version mismatch or supported
	CBM_STUB_INVALIDATE_ERROR, // invalidation of stubbed file failed
	CBM_CLAPI_PARTIAL_FILE,
	CBM_CACHEINFO_SIZE_ERROR, // cache info read which is not satisifed by the recorded cache size
	CBM_FILE_TRUNCATED, // indicate a file has been truncated while reading the cache
	CBM_CLAPI_AUTHORIZATION_FAILED,
	CBM_VECTORED_IO_ERROR, // unable to perform a vectored IO operation
	CBM_CACHE_MARK_ERROR, // Failed to mark the ADS cacheinfo file.
	CBM_COI_VERSION_MISMATCH, // version mismatch or supported.
	CBM_VERSION_MISMATCH, // version mismatch or supported.
	CBM_ARCHIVE_CKPT_VERSION_MISMATCH, // version mismatch or supported.
	CBM_MAPINFO_VERSION_MISMATCH, // version mismatch or supported.
	CBM_CMO_VERSION_MISMATCH, // version mismatch for CMO.
	CBM_STUBMAP_INVALID_HEADERSIZE, // headersize mismatch on extract
	CBM_NEED_LOCK_UPGRADE, // Retry operation with exclusive lock
	CBM_SERIALIZATION_ERROR,
	CBM_DESERIALIZATION_ERROR,
	CBM_SYNC_ALREADY_IN_PROGRESS,
	CBM_CMOI_VERSION_MISMATCH, // CMO index entry version mismatch.
	CBM_CMOI_WRONG_LENGTH, // CMO index entry has wrong length.
	CBM_CMOI_CORRUPT_ENTRY, // CMO index entry is corrupted.
	CBM_RECALL_CKPT_MAGIC_MISMATCH, //magic mismatch
	CBM_RECALL_CKPT_VERSION_MISMATCH, //version mismatch
	CBM_RECALL_CKPT_INVALID, //invalid recall ckpt
	CBM_SPARSE_VEC_TOO_BIG, //too many sparse regions force iovec overflows
	CBM_ERROR_SHORT_SPARSE_DATA, // not enough sparse regions for data
	CBM_NOT_IN_PERMIT_LIST, // Cluster guid is not enabled in permit list
	CBM_ERROR_SYNC_DURING_UPGRADE, //sync not allowed during upgrade
	CBM_RESTRICTED_MODIFY, // modification is not allowed currently
	MAX_CBM_ERROR_TYPE // must be last
};

typedef enum cbm_error_type_values cbm_error_type;

struct isi_cbm_error;

#define ISI_CBM_ERROR_PROTECTED
#include "isi_cbm_error_protected.h"
#undef ISI_CBM_ERROR_PROTECTED

ISI_ERROR_CLASS_DECLARE(CBM_ERROR_CLASS);

cbm_error_type
isi_cbm_error_get_type(const struct isi_error *error);

static inline bool
isi_cbm_error_is_a(const struct isi_error *error, cbm_error_type type)
{
	if (error == NULL)
		return false;
	return isi_error_is_a(error, CBM_ERROR_CLASS) &&
	    isi_cbm_error_get_type(error) == type;
}

struct fmt_conv_ctx cbm_error_type_fmt(enum cbm_error_type_values);

__END_DECLS

#endif //  __ISI_CBM_ERROR_H__
