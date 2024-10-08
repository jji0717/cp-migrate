
/*
 * This file is auto-generated by isi_cbm_error_gen.py.  Do NOT edit this
 * file directly!
 */

#include "isi_cbm_error.h"

MAKE_ENUM_FMT(cbm_error_type, enum cbm_error_type_values,
	ENUM_VAL(CBM_SUCCESS),
	ENUM_VAL(CBM_STD_EXCEPTION_FAILURE),
	ENUM_VAL(CBM_ISI_EXCEPTION_FAILURE),
	ENUM_VAL(CBM_CLAPI_COULDNT_CONNECT),
	ENUM_VAL(CBM_CLAPI_CONNECT_DENIED),
	ENUM_VAL(CBM_CLAPI_AUTHENTICATION_FAILED),
	ENUM_VAL(CBM_CLAPI_ACCOUNT_IS_DISABLED),
	ENUM_VAL(CBM_CLAPI_SERVER_BUSY),
	ENUM_VAL(CBM_CLAPI_SERVER_ERROR),
	ENUM_VAL(CBM_CLAPI_NETWORK_RW_FAILED),
	ENUM_VAL(CBM_CLAPI_INVALID_KEY),
	ENUM_VAL(CBM_CLAPI_OUT_OF_MEMORY),
	ENUM_VAL(CBM_CLAPI_TIMED_OUT),
	ENUM_VAL(CBM_CLAPI_ABORTED_BY_CALLBACK),
	ENUM_VAL(CBM_CLAPI_CALLBACK_RW_FAILED),
	ENUM_VAL(CBM_CLAPI_CONTAINER_EXISTS),
	ENUM_VAL(CBM_CLAPI_CONTAINER_BEING_DELETED),
	ENUM_VAL(CBM_CLAPI_CONTAINER_NOT_FOUND),
	ENUM_VAL(CBM_CLAPI_OBJECT_NOT_FOUND),
	ENUM_VAL(CBM_CLAPI_OBJECT_EXISTS),
	ENUM_VAL(CBM_CLAPI_BAD_REQUEST),
	ENUM_VAL(CBM_CLAPI_UNEXPECTED),
	ENUM_VAL(CBM_LIMIT_EXCEEDED),
	ENUM_VAL(CBM_INTEGRITY_FAILURE),
	ENUM_VAL(CBM_PERM_ERROR),
	ENUM_VAL(CBM_STALE_STUB_ERROR),
	ENUM_VAL(CBM_NOSYS_ERROR),
	ENUM_VAL(CBM_INVALID_PARAM_ERROR),
	ENUM_VAL(CBM_NOT_SUPPORTED_ERROR),
	ENUM_VAL(CBM_FILE_TYPE_ERROR),
	ENUM_VAL(CBM_READ_ONLY_ERROR),
	ENUM_VAL(CBM_CACHE_HEADER_ERROR),
	ENUM_VAL(CBM_CACHE_EXTEND_ERROR),
	ENUM_VAL(CBM_CACHEINFO_ERROR),
	ENUM_VAL(CBM_NO_REGION_ERROR),
	ENUM_VAL(CBM_ALREADY_STUBBED),
	ENUM_VAL(CBM_NOT_A_STUB),
	ENUM_VAL(CBM_MAP_EMPTY),
	ENUM_VAL(CBM_MAP_ENTRY_NOT_FOUND),
	ENUM_VAL(CBM_CDO_WRITE_FAILURE),
	ENUM_VAL(CBM_LIN_EXISTS),
	ENUM_VAL(CBM_LIN_DOES_NOT_EXIST),
	ENUM_VAL(CBM_NO_IO_HELPER),
	ENUM_VAL(CBM_STUBMAP_MISMATCH),
	ENUM_VAL(CBM_SIZE_MISMATCH),
	ENUM_VAL(CBM_MAP_INFO_ERROR),
	ENUM_VAL(CBM_SECT_HEAD_ERROR),
	ENUM_VAL(CBM_SYNC_REC_ERROR),
	ENUM_VAL(CBM_MOD_REC_ERROR),
	ENUM_VAL(CBM_INVALID_ACCOUNT),
	ENUM_VAL(CBM_INVALID_CACHE),
	ENUM_VAL(CBM_NO_MAP_ENTRY),
	ENUM_VAL(CBM_FILE_PAUSED),
	ENUM_VAL(CBM_FILE_CANCELLED),
	ENUM_VAL(CBM_FILE_STOPPED),
	ENUM_VAL(CBM_DOMAIN_LOCK_FAILURE),
	ENUM_VAL(CBM_DOMAIN_LOCK_CONTENTION),
	ENUM_VAL(CBM_INVALIDATE_FILE_DIRTY),
	ENUM_VAL(CBM_CHECKSUM_ERROR),
	ENUM_VAL(CBM_CACHE_NO_HEADER),
	ENUM_VAL(CBM_CACHE_VERSION_MISMATCH),
	ENUM_VAL(CBM_STUB_INVALIDATE_ERROR),
	ENUM_VAL(CBM_CLAPI_PARTIAL_FILE),
	ENUM_VAL(CBM_CACHEINFO_SIZE_ERROR),
	ENUM_VAL(CBM_FILE_TRUNCATED),
	ENUM_VAL(CBM_CLAPI_AUTHORIZATION_FAILED),
	ENUM_VAL(CBM_VECTORED_IO_ERROR),
	ENUM_VAL(CBM_CACHE_MARK_ERROR),
	ENUM_VAL(CBM_COI_VERSION_MISMATCH),
	ENUM_VAL(CBM_VERSION_MISMATCH),
	ENUM_VAL(CBM_ARCHIVE_CKPT_VERSION_MISMATCH),
	ENUM_VAL(CBM_MAPINFO_VERSION_MISMATCH),
	ENUM_VAL(CBM_CMO_VERSION_MISMATCH),
	ENUM_VAL(CBM_STUBMAP_INVALID_HEADERSIZE),
	ENUM_VAL(CBM_NEED_LOCK_UPGRADE),
	ENUM_VAL(CBM_SERIALIZATION_ERROR),
	ENUM_VAL(CBM_DESERIALIZATION_ERROR),
	ENUM_VAL(CBM_SYNC_ALREADY_IN_PROGRESS),
	ENUM_VAL(CBM_CMOI_VERSION_MISMATCH),
	ENUM_VAL(CBM_CMOI_WRONG_LENGTH),
	ENUM_VAL(CBM_CMOI_CORRUPT_ENTRY),
	ENUM_VAL(CBM_RECALL_CKPT_MAGIC_MISMATCH),
	ENUM_VAL(CBM_RECALL_CKPT_VERSION_MISMATCH),
	ENUM_VAL(CBM_RECALL_CKPT_INVALID),
	ENUM_VAL(CBM_SPARSE_VEC_TOO_BIG),
	ENUM_VAL(CBM_ERROR_SHORT_SPARSE_DATA),
	ENUM_VAL(CBM_NOT_IN_PERMIT_LIST),
	ENUM_VAL(CBM_ERROR_SYNC_DURING_UPGRADE),
	ENUM_VAL(CBM_RESTRICTED_MODIFY)
);
