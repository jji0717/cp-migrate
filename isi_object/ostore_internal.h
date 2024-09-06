#ifndef __OSTORE_INTERNAL_H_
#define __OSTORE_INTERNAL_H__

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Copyright (c) 2011
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	ostore_internal.h __ 2011/08/31 12:10:31 PDT __ jonathan
 */
/** @addtogroup ostore_private
 * @{ */
/** @file
 * Header file for the internal object store API
 */

#include <isi_ilog/ilog.h>

#include <isi_object/ostore.h>
#include "common.h"

#define OBJECT_STORE_ROOTNAME	"/ifs/.object"
#define OBJECT_STORE_TMP        OBJECT_STORE_ROOTNAME"/tmp"
#define OBJECT_STORE_VERSION    1
 
#define NS_STORE_ROOT           "/ifs/.ifsvar/namespace"
#define NS_STORE_TMP            NS_STORE_ROOT"/tmp"
#define NS_STORE_AP_ROOT        NS_STORE_ROOT"/access_points"
#define NS_STORE_AP_BPATH       ".ifsvar/namespace/access_points"
#define NS_STORE_VERSION        1

/*XXX for now, just a mirror of the public... until we properly split things */


/* ostore_error.c */
#define ostore_error_new(args...) \
    _ostore_error_new(__FUNCTION__, __FILE__, __LINE__, "" args)
struct isi_error *_ostore_error_new(const char *, const char *, int, 
    const char *, ...) __nonnull();

#define ostore_invalid_argument_error_new(args...)			\
    _ostore_invalid_argument_error_new(__FUNCTION__, __FILE__, __LINE__, \
	"" args)
struct isi_error *_ostore_invalid_argument_error_new(const char *, 
    const char *, int, const char *, ...) __nonnull();

#define ostore_object_not_found_error_new(args...)			\
    _ostore_object_not_found_error_new(__FUNCTION__, __FILE__, __LINE__, \
	"" args)
struct isi_error *_ostore_object_not_found_error_new(const char *, 
    const char *, int, const char *, ...) __nonnull();

#define ostore_attribute_not_found_error_new(args...)			\
    _ostore_attribute_not_found_error_new(__FUNCTION__, __FILE__, __LINE__, \
"" args)
struct isi_error *_ostore_attribute_not_found_error_new(const char *, 
    const char *, int, const char *, ...) __nonnull();

#define ostore_store_not_mounted_error_new(args...)			\
    _ostore_store_not_mounted_error_new(__FUNCTION__, __FILE__, __LINE__, \
"" args)
struct isi_error *_ostore_store_not_mounted_error_new(const char *, 
    const char *, int, const char *, ...) __nonnull();

#define ostore_invalid_handle_error_new(args...)			\
    _ostore_invalid_handle_error_new(__FUNCTION__, __FILE__, __LINE__, "" args)
struct isi_error *_ostore_invalid_handle_error_new(const char *, 
    const char *, int, const char *, ...) __nonnull();

#define ostore_access_denied_error_new(args...)			\
    _ostore_access_denied_error_new(__FUNCTION__, __FILE__, __LINE__, "" args)
struct isi_error *_ostore_access_denied_error_new(const char *, 
    const char *, int, const char *, ...) __nonnull();

#define ostore_bucket_exists_error_new(args...)			\
    _ostore_bucket_exists_error_new(__FUNCTION__, __FILE__, __LINE__, "" args)
struct isi_error *_ostore_bucket_exists_error_new(const char *, 
    const char *, int, const char *, ...) __nonnull();
    
#define ostore_object_exists_error_new(args...)			\
    _ostore_object_exists_error_new(__FUNCTION__, __FILE__, __LINE__, "" args)
struct isi_error *_ostore_object_exists_error_new(const char *, 
    const char *, int, const char *, ...) __nonnull();

#define ostore_out_of_range_error_new(args...)			\
    _ostore_out_of_range_error_new(__FUNCTION__, __FILE__, __LINE__, "" args)
struct isi_error *_ostore_out_of_range_error_new(const char *, 
    const char *, int, const char *, ...) __nonnull();

#define ostore_exceeds_limit_error_new(args...)			\
    _ostore_exceeds_limit_error_new(__FUNCTION__, __FILE__, __LINE__, "" args)
struct isi_error *_ostore_exceeds_limit_error_new(const char *, 
    const char *, int, const char *, ...) __nonnull();

#define ostore_invalid_license_error_new(args...)			\
    _ostore_invalid_license_error_new(__FUNCTION__, __FILE__, __LINE__, "" args)
struct isi_error *_ostore_invalid_license_error_new(const char *, 
    const char *, int, const char *, ...) __nonnull();

#define ostore_unsupported_object_type_error_new(args...)		\
    _ostore_unsupported_object_type_error_new(__FUNCTION__, __FILE__, __LINE__,\
    "" args)
struct isi_error *_ostore_unsupported_object_type_error_new(const char *,
    const char *, int, const char *, ...) __nonnull();

#define ostore_pathname_not_exist_error_new(args...)		\
    _ostore_pathname_not_exist_error_new(__FUNCTION__, __FILE__, __LINE__,\
    "" args)
struct isi_error *_ostore_pathname_not_exist_error_new(const char *,
    const char *, int, const char *, ...) __nonnull();

#define ostore_pathname_not_onefs_error_new(args...)		\
    _ostore_pathname_not_onefs_error_new(__FUNCTION__, __FILE__, __LINE__,\
    "" args)
struct isi_error *_ostore_pathname_not_onefs_error_new(const char *,
    const char *, int, const char *, ...) __nonnull();

#define ostore_pathname_not_dir_error_new(args...)		\
    _ostore_pathname_not_dir_error_new(__FUNCTION__, __FILE__, __LINE__,\
    "" args)
struct isi_error *_ostore_pathname_not_dir_error_new(const char *,
    const char *, int, const char *, ...) __nonnull();

#define ostore_pathname_prohibited_error_new(args...)		\
    _ostore_pathname_prohibited_error_new(__FUNCTION__, __FILE__, __LINE__,\
    "" args)
struct isi_error *_ostore_pathname_prohibited_error_new(const char *,
    const char *, int, const char *, ...) __nonnull();

#define ostore_copy_to_sub_dir_error_new(args...)		\
    _ostore_copy_to_sub_dir_error_new(__FUNCTION__, __FILE__, __LINE__,\
    "" args)
struct isi_error *_ostore_copy_to_sub_dir_error_new(const char *,
    const char *, int, const char *, ...) __nonnull();

/**
 * Release the attributes structure returned by iobj_ostore_privattrs_get
 * once it is no longer needed. 
 */
void
iobj_ostore_parameters_release(struct ostore_handle *ios,
    struct ostore_parameters *params);

/**
 * Get the private (non-consumer) store related attributes that effect all
 * buckets in the store.  These attributes are currently meant for testing
 * purposes only.
 * If any attribute is to be changed for the store, then this get routine MUST
 * be called first, only the attribute to be changed is set in the attrs
 * structure and then the companion set function is called to change the
 * attributes.  This allows us to conserve the default attributes if they are
 * not changed.
 * @param ios open handle to the store for which to get that attributes.
 * @param attrs structure where the attributes will be set.
 * @param error isi_error structure, NULL if success, set if some type of error
 */
void
iobj_ostore_parameters_set(struct ostore_handle *ios, 
    struct ostore_parameters *params, struct isi_error **error_out);

/**
 * Get the private (non-consumer) store related attributes that effect all
 * buckets in the store.  These attributes are currently meant for testing
 * purposes only.
 * If any attribute is to be changed for the store, then the get routine MUST
 * be called first, only the attribute to be changed is set in the attrs
 * structure and then then this is called to change the attributes.  This
 * allows us to conserve the default attributes if they are not changed. 
 * @param ios open handle to the store for which to get that attributes.
 * @param attrs structure where the attributes will be set.
 * @param error isi_error structure, NULL if success, set if some type of error
 */
void
iobj_ostore_parameters_get(struct ostore_handle *ios, 
    struct ostore_parameters **params, struct isi_error **error_out);

/** @} */
#ifdef __cplusplus
}
#endif

#endif /* __OSTORE_INTERNAL_H__ */
