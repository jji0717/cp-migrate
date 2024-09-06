/*
 * Copyright (c) 2012
 *	Isilon Systems, LLC.  All rights reserved.
 *
 *	genobj_acl.c __ lchampakesan.
 */
#include <string.h>
#include <isi_ilog/ilog.h>
#include <sys/isi_acl.h>
#include <sys/isi_persona.h>
#include <isi_acl/isi_acl_util.h>

#include "genobj_acl.h"
#include "ostore_internal.h"


/**
 * This routine is used to get the security descriptor of the entity 
 * whose file descriptor is passed as input. 
 * The entity can be Namespace directory or file or 
 * Object store bucket or object.
 */
void 
get_security_descriptor(struct igenobj_handle *igobj_hdl, 
    enum ifs_security_info sec_info, struct ifs_security_descriptor **sd_out,  
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct ifs_sec_desc_buf sdb = {};

	int err = 0;

	sdb.sd = NULL;
	// Get the security descriptor for the file descriptor passed.
	err = aclu_get_fd_sd(igobj_hdl->fd, sec_info, &sdb);
	if (err) {
		goto out;
	}

out:
	if (err) {
		switch (err) {
		case ENOENT:
			error = ostore_object_not_found_error_new( 
				"%s does not exists", igobj_hdl->name);
			break;
		case EACCES:
			error = ostore_access_denied_error_new(
				"Denied access to %s ", igobj_hdl->name);
			break;
		case ENOMEM:
			error = isi_system_error_new(errno, 
			    "Cannot get ACL for the file %s. " 
			    "Memory allocation error.", 
			    igobj_hdl->name);
			break;
		case EINVAL:
			error = isi_system_error_new(errno,
			    "Internal error while getting "
			    "security descriptor of %s",
			    igobj_hdl->name);
			break;
		default:
			error = isi_system_error_new(errno,
			    "Unable to access %s", igobj_hdl->name);
			break;
		}
	}

	if (error) {
		isi_error_handle(error, error_out);
	}

	// Pass the security descriptor back to the caller.
	*sd_out = sdb.sd;
	
	return;
}


/**
 * This routine modifies the security descriptor of the entity 
 * whose file descriptor is passed as input. The ace's to be 
 * modified are passed in the form of struct ifs_ace_mod.
 * The entity can be Namespace directory or file or 
 * Object store bucket or object.
 */
void 
modify_security_descriptor(struct igenobj_handle *igobj_hdl,
    struct ifs_ace_mod *mods, int num_mods, 
    enum ifs_acl_mod_type acl_mod_type, 
    enum ifs_security_info sec_info,
    struct ifs_security_descriptor **sd_out,
    struct isi_error **error_out) 
{
	struct isi_error *error = NULL;
	int err = 0;

	// Set the new ace's in the security descriptor.
	err = ifs_modify_sd(igobj_hdl->fd, mods, num_mods, 
	    acl_mod_type, sec_info, sd_out);
	if (err) {
		goto out;
	}
		
out:
	if (err) {
		switch (err) {
			case ENOENT:
				error = ostore_object_not_found_error_new( 
				    "%s does not exists", igobj_hdl->name);
				break;
			case EACCES:
				error = ostore_access_denied_error_new(
				    "Denied access to %s ", igobj_hdl->name);
				break;
            		case ENOMEM:
				error = isi_system_error_new(errno, 
				    "Cannot set ACL for the file %s. " 
                                    "Memory allocation error.", 
				    igobj_hdl->name);
				break;
		        case EINVAL:
				error = isi_system_error_new(errno,
				    "Internal error while setting "
				    "security descriptor of %s",
				    igobj_hdl->name);
				break;
		        default:
				error = isi_system_error_new(errno,
				    "Unable to access %s", igobj_hdl->name);
				break;
			}
	}

	if (error) {
		isi_error_handle(error, error_out);
	}


	return;
}


/**
 * This routine is used to set the security descriptor of the entity 
 * whose the file descriptor is passed as input. 
 * The entity can be Namespace directory or file or 
 * Object store bucket or object.
 */
void 
set_security_descriptor(struct igenobj_handle *igobj_hdl, 
    struct ifs_security_descriptor *sd,  
    enum ifs_security_info sec_info, 
    struct isi_error **error_out)
{

	struct isi_error *error = NULL;
	int err = 0;
	
	// Set the security descriptor for the file descriptor.
	err = aclu_set_fd_sd(igobj_hdl->fd, sec_info, sd, 
	    SMAS_FLAGS_FORCE_SD);
	if (err) {
		goto out;
	}

out:
	if (err) {
		switch (err) {
		case ENOENT:
			error = ostore_object_not_found_error_new( 
				"%s does not exists", igobj_hdl->name);
			break;
		case EACCES:
			error = ostore_access_denied_error_new(
				"Denied access to %s ", igobj_hdl->name);
			break;
		case ENOMEM:
			error = isi_system_error_new(errno, 
			    "Cannot get ACL for the file %s. " 
			    "Memory allocation error.", 
			    igobj_hdl->name);
			break;
		case EINVAL:
			error = isi_system_error_new(errno,
			    "Internal error while setting "
			    "security descriptor of %s",
			    igobj_hdl->name);
			break;
		default:
			error = isi_system_error_new(errno,
			    "Unable to access %s", igobj_hdl->name);
			break;
		}
	}

	if (error) {
		isi_error_handle(error, error_out);
	}
	
	return;
}


void
set_access_control(struct igenobj_handle *igobj_hdl,
    enum ostore_acl_mode acl_mode, int mode, uid_t uid, gid_t gid,
    struct isi_error **error_out)
{
	ASSERT(igobj_hdl && igobj_hdl->fd);
	struct isi_error *error = NULL;

	if (uid != (uid_t) -1 || gid != (uid_t) -1) {
		if (fchown(igobj_hdl->fd, uid, gid)) {
			error = isi_system_error_new(errno,
			    "failed to change owner or group");
			goto out;
		}
	}
	// Set acl only if acl_mode is specified.
	if (acl_mode != OSTORE_ACL_NONE) {
		// Set the acl on the file.
		ostore_set_default_acl(NULL, igobj_hdl->fd,
		    acl_mode, NULL, &error);

		if (error)
			goto out;
	} else if (mode != (mode_t) -1) {
		if (fchmod(igobj_hdl->fd, mode)) {
			// this is internal error ?
			error = isi_system_error_new(errno,
			    "failed to set permission");
			goto out;
		}
	}

out:
	if (error) {
		isi_error_handle(error, error_out);
	}

}

