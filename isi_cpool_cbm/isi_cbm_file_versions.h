#ifndef _ISI_CBM_FILE_VERSIONS_H_
#define _ISI_CBM_FILE_VERSIONS_H_

#include <sys/types.h>

__BEGIN_DECLS

/**
 * All capabilities supported by cbm in this release.  More capabilities can be
 * added or existing capabilities removed in the next release.
 */
#define HAS_RAN_PROVIDER			0x0001
#define HAS_AZURE_PROVIDER			0x0002
#define HAS_S3_PROVIDER				0x0004
#define HAS_COMP_V1				0x0008
#define HAS_ENCR_V1				0x0010
#define HAS_ECS_PROVIDER			0x0020
#define HAS_V4_AUTH_VER_ONLY			0x0040
#define HAS_GOOGLE_XML_PROVIDER			0x0100

#define CBM_FILE_VER_RIPTIDE_NUM	1
#define CBM_FILE_VER_RIPTIDE_FLAGS (HAS_RAN_PROVIDER | HAS_AZURE_PROVIDER | \
    HAS_S3_PROVIDER| HAS_COMP_V1 | HAS_ENCR_V1 | HAS_ECS_PROVIDER)

/**
 * we only bump the file version number if key information, such as mapinfo,
 * is modified. With expanded capability, file version stays the same, just
 * update the flags with new capabilities
 */
#define CBM_FILE_VER_NIIJIMA_FLAGS	(CBM_FILE_VER_RIPTIDE_FLAGS | \
    HAS_GOOGLE_XML_PROVIDER)

/**
 * A cbm file version
 */
struct isi_cbm_file_version {	// logically opaque
	uint64_t _num;
	uint64_t _flags;
};


/**
 * A collection of cbm file versions
 */
struct isi_cbm_file_versions;	// opaque


/**
* Create an empty version set.
*
* @return Created version collection on success, must be destroyed after use;
*     NULL on failure
*/
struct isi_cbm_file_versions *isi_cbm_file_versions_create(void);

/**
* Get the version set supported by this release at the called instant. The
* supported version set depends on CP license and if no license is available
* an empty version is returned.
*
* @return Created version collection on success, must be destroyed after use;
*     NULL on failure
*/
struct isi_cbm_file_versions *isi_cbm_file_versions_get_supported(void);

/**
* Destroy cbm file version collection.
*
* @param vs[IN]:	version collection to be destroyed
*/
void isi_cbm_file_versions_destroy(struct isi_cbm_file_versions *vs);

/**
* Obtain a collection of versions that are common to the provided
* version collections
*
* @param vs1[IN]:	version collection 1
* @param vs1[IN]:	version collection 2
*
* @return The common version collection, if any;  NULL otherwise. Must
* be destroyed after use.
*/
struct isi_cbm_file_versions *isi_cbm_file_versions_common(
    const struct isi_cbm_file_versions *vs1,
    const struct isi_cbm_file_versions *vs2);

/**
* Obtain a transportable/storable byte representation of the
* provided collection
*
* @param vs[IN]:	the version collection
* @param buf[OUT]:	pointer to the buffer where serialized content is put
* @param nbytes[IN]:	number of bytes available for the
*                       serializable content in the buffer
* @param error_out[OUT]:errors if any
*
* @return The number of bytes that are used on success or the number
*         of total bytes that are needed on failure. An error is always
          returned on failure.
*/
size_t isi_cbm_file_versions_serialize(const struct isi_cbm_file_versions *vs,
    void *buf, size_t nbytes, struct isi_error **error_out);

/**
* Obtain a version collection from the transportable/storable byte
* representation thereof
*
* @param buf[IN]:	pointer to the the serialized content
* @param nbytes[IN]:	number of bytes for the serialized content
* @param error_out[OUT]:errors if any

* @return the created version collection on success; NULL otherwise. Must
* be destroyed after use.
*/
struct isi_cbm_file_versions *isi_cbm_file_versions_deserialize(
    const void *buf, size_t nbytes, struct isi_error **error_out);

/**
* Obtain a transportable/storable byte representation of a version
*
* @param v[IN]:		the version
* @param buf[OUT]:	pointer to the buffer where serialized content is put
* @param nbytes[IN]:	number of bytes available for the
*                       serializable content in the buffer
* @param error_out[OUT]:errors if any
*
* @return The number of bytes that are used on success or the number
*         of total bytes that are needed on failure. An error is always
          returned on failure.
*/
size_t
isi_cbm_file_version_serialize(const struct isi_cbm_file_version *v,
    void *buf, size_t nbytes, struct isi_error **error_out);

/**
* Obtain a version from the transportable/storable byte
* representation thereof
* 
* @param v[OUT]:	the version
* @param buf[IN]:	pointer to the the serialized content
* @param nbytes[IN]:	number of bytes for the serialized content
* @param error_out[OUT]:errors if any
*/
void 
isi_cbm_file_version_deserialize(struct isi_cbm_file_version *v,
    const void *buf, size_t nbytes,
    struct isi_error **error_out);

/**
* Check if a version exists in the provided version collection
*
* @param v[IN]:		the version to check
* @param vs[IN]:	the version collection to check the version against
*
* @return true if version exists in the collection; false otherwise
*/
bool isi_cbm_file_version_exists(const struct isi_cbm_file_version *v,
    const struct isi_cbm_file_versions *vs);


/* Test use ONLY */
void supported_versions_set(struct isi_cbm_file_version v[], size_t num);
void supported_versions_clear(void);
void isi_cbm_file_versions_license(bool available);

__END_DECLS

#endif // _ISI_CBM_FILE_VERSIONS_H_
