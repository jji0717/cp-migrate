#ifndef __LIB__ISI_OBJECT__IOBJ_OSTORE_PROTOCOL__H_
#define __LIB__ISI_OBJECT__IOBJ_OSTORE_PROTOCOL__H_

#include <sys/isi_acl.h>
#include <sys/isi_persona.h>

#include "ostore.h"

#ifdef __cplusplus
extern "C" {
#endif

#define BUCKET_READ                 (IFS_RTS_DIR_LIST                 | \
                                     IFS_RTS_DIR_READ_EA              | \
                                     IFS_RTS_DIR_READ_ATTRIBUTES      | \
	                             IFS_RTS_DIR_TRAVERSE             | \
                                     IFS_RTS_STD_SYNCHRONIZE)

#define CONTAINER_READ              BUCKET_READ

#define BUCKET_WRITE                (IFS_RTS_DIR_ADD_FILE             | \
                                     IFS_RTS_DIR_ADD_SUBDIR           | \
                                     IFS_RTS_DIR_WRITE_EA             | \
                                     IFS_RTS_DIR_DELETE_CHILD         | \
                                     IFS_RTS_DIR_WRITE_ATTRIBUTES     | \
	                             IFS_RTS_DIR_TRAVERSE             | \
		                     IFS_RTS_STD_SYNCHRONIZE)

#define BUCKET_OWNER_WRITE          (BUCKET_WRITE                     | \
                                     IFS_RTS_STD_DELETE)


#define BUCKET_FULLACCESS	    (BUCKET_READ | BUCKET_WRITE       | \
	                             IFS_RTS_STD_READ_CONTROL         | \
                                     IFS_RTS_STD_WRITE_DAC)

#define BUCKET_OWNER_FULLACCESS     (BUCKET_FULLACCESS                | \
	                             IFS_RTS_STD_DELETE)

#define OSTORE_READ_ACL             (IFS_RTS_STD_READ_CONTROL)

#define CONTAINER_RWRITE            (IFS_RTS_DIR_ADD_FILE             | \
	                             IFS_RTS_DIR_ADD_SUBDIR)

#define BUCKET_GEN_WRITE            (IFS_RTS_DIR_ADD_FILE             | \
                                     IFS_RTS_DIR_ADD_SUBDIR           | \
                                     IFS_RTS_DIR_WRITE_EA             | \
                                     IFS_RTS_DIR_WRITE_ATTRIBUTES     | \
	                             IFS_RTS_DIR_TRAVERSE             | \
		                     IFS_RTS_STD_SYNCHRONIZE) 

/*
 * Before writing the ACL, the ACL is read to get the latest ACL info
 * for in-memory modifications. So writing ACL also requires read ACL 
 * access.
*/
#define OSTORE_WRITE_ACL            (OSTORE_READ_ACL                  | \
	                             IFS_RTS_STD_WRITE_DAC)

#define OBJECT_READ                 (IFS_RTS_FILE_READ_DATA           | \
                                     IFS_RTS_FILE_READ_EA             | \
                                     IFS_RTS_FILE_READ_ATTRIBUTES     | \
	                             IFS_RTS_FILE_EXECUTE             | \
                                     IFS_RTS_STD_SYNCHRONIZE)


#define TMP_OBJECT_WRITE            (IFS_RTS_FILE_WRITE_DATA          | \
                                     IFS_RTS_FILE_APPEND_DATA         | \
                                     IFS_RTS_FILE_WRITE_EA            | \
                                     IFS_RTS_FILE_WRITE_ATTRIBUTES    | \
	                             IFS_RTS_FILE_EXECUTE             | \
                                     IFS_RTS_STD_SYNCHRONIZE)

#define OBJECT_FULLACCESS           (OBJECT_READ                      | \
                                     OSTORE_READ_ACL                  | \
                                     OSTORE_WRITE_ACL)

void protocol_set_defaultacl_object_file (const char *fullpath, 
    struct iobject_handle *iobj, 
    struct persona **owner,
    struct isi_error **error_out);

bool check_listbucket_condition(const char *bucket_path, 
    struct persona *login_user, struct isi_error **error_out);

#ifdef __cplusplus
}
#endif

/** @} */
#endif /* __LIB_ISI_OBJECT__IOBJ_OSTORE_PROTOCOL__H_ */
