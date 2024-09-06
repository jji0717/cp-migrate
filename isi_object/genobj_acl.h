/*
 * Copyright (c) 2012
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	genobj_acl.h __ lchampakesan.
 */

#ifndef __GENOBJ_ACL_H__
#define __GENOBJ_ACL_H__

#include <sys/isi_acl.h>
#include <sys/isi_persona.h>
#include <isi_acl/isi_sd.h>
#include "common.h"


/*
 * This routine retrieves the acl of the Bucket/Object.
 * @param[IN] igobj_hdl - Generic object handle.
 * @param[IN] sec_info - security information. 
 * @param[OUT] sd_out the security descriptor after modification based on 
 *                 user input.
 * @param[OUT] error_out error output.
 */
void get_security_descriptor(struct igenobj_handle *igobj_hdl,
    enum ifs_security_info sec_info, 
    struct ifs_security_descriptor **sd_out,
    struct isi_error **error_out);

/**
 * This routine modifies the security descriptor based on the 
 * modifications input by the user.
 *
 * @param[IN] igobj_hdl - Generic object handle.
 * @param[IN] mods - modifications to the acl input by the user.
 * @param[IN] num_mods - number of ace's (to be added/modified/deleted).
 * @param[IN] acl_mod_type indicates if ace's are to be replaced or modified.
 * @param[IN] sec_info - security information. 
 * @param[OUT] sd_out the security descriptor after modification based on 
 *                 user input.
 * @param[OUT] error_out error output.
 */
void 
modify_security_descriptor(struct igenobj_handle *igobj_hdl,
    struct ifs_ace_mod *mods, int num_mods, 
    enum ifs_acl_mod_type acl_mod_type, 
    enum ifs_security_info sec_info,
    struct ifs_security_descriptor **sd_out,
    struct isi_error **error_out);


/*
 * This routine stores the acl of the Bucket/Object.
 *
 * @param[IN] igobj_hdl - Generic object handle.
 * @param[IN] sd - security descriptor.
 * @param[IN] sec_info - security information.
 * @param[OUT] error_out error output.
 */
void set_security_descriptor(struct igenobj_handle *igobj_hdl,
    struct ifs_security_descriptor *sd,
    enum ifs_security_info sec_info,
    struct isi_error **error_out);


/**
 * Set acl or unix permission mode and ownership for the object.
 *
 * The mode is used only when @acl_mode is OSTORE_ACL_NONE
 *
 * If mode or uid or gid has value of -1, then corresponding
 * unix permission mode or owner user id or group id will not
 * be changed.
 *
 * @param[IN] igobj_hdl - General object handle.
 * @param[IN] acl_mode - predefined ACL mode
 * @param[IN] mode - access mode
 * @param[IN] uid - user id
 * @param[IN] gid - group id
 * @param[OUT] error - the error on failure
 */
void set_access_control(struct igenobj_handle *igobj_hdl,
    enum ostore_acl_mode acl_mode, int mode, uid_t uid, gid_t gid,
    struct isi_error **error_out);

/** @} */
#endif /* __GENOBJ_ACL_H__ */
