/*
 * oapi_genobj.cpp
 *
 *  Created on: Mar 8, 2012
 *      Author: lwang2
 */

#include "oapi_genobj.h"

void
oapi_genobj::close()
{
	if (handle_) {
		iobj_genobj_close(handle_,
		    isi_error_suppress());
		handle_ = NULL;
	}
}

oapi_genobj::~oapi_genobj()
{
	try {
		if (handle_) {
			close();
		}

	}
	catch (...) {
		// swallow the exceptions if any
	}
}


OSTORE_OBJECT_TYPE
oapi_genobj::get_object_type()
{
	if (handle_ != NULL)
		return iobj_genobj_get_type(handle_);
	return OOT_INVALID;
}

const char *
oapi_genobj::get_name()
{
	if (handle_ != NULL)
		return iobj_genobj_get_name(handle_, isi_error_suppress());
	return NULL;
}

void
oapi_genobj::remove(const char *path, ostore_del_flags flags, 
    isi_error *&error)
{
	iobj_genobj_delete(store_.get_handle(), path, flags, &error);
}


int
oapi_genobj::copy(const char *acct_name,
    struct ostore_copy_param *cp_param, isi_error *&error)
{
	ASSERT(!error);
	return iobj_genobj_copy(cp_param, &error);
}

int
oapi_genobj::move(const char *acct_name, struct ostore_handle *src_ios,
    const char *src, struct ostore_handle *dest_ios,
    const char *dest, struct fsu_move_flags *mv_flags, isi_error *&error)
{
	ASSERT(!error);
	return iobj_genobj_move(src_ios, src, dest_ios, dest, mv_flags, &error);
}

/*
 * This routine gets the acl of the entity whose handle 
 * is passed in the ifs_sd_trustee struct.
 * The bucket or the container is already open when this routine 
 * is invoked. The handle_ will have the opened object information.
 */

void
oapi_genobj::get_acl(enum ifs_security_info sec_info, 
    struct ifs_security_descriptor **sd_out,
    isi_error *&error )
{
	*sd_out = NULL;
	ASSERT(handle_ != NULL);

	iobj_genobj_acl_get(handle_, sec_info, sd_out, &error);
}


/*
 * This routine gets the security descriptor of the file descriptor 
 * and modifies the security descriptor based on modifications passed 
 * as input by the user. 
 */
void
oapi_genobj::modify_sd(struct ifs_ace_mod *mods, int num_mods, 
    enum ifs_acl_mod_type acl_mod_type,
    enum ifs_security_info sec_info,
    struct ifs_security_descriptor **sd_out, isi_error *&error) 
{
	ASSERT(handle_ != NULL);

	iobj_genobj_modify_sd(handle_, mods, num_mods, acl_mod_type, 
	    sec_info, sd_out, &error);
}


/*
 * This routine puts the acl of the entity whose handle 
 * is passed in the ifs_sd_trustee struct.
 * The bucket or the container is already open when this routine 
 * is invoked. The handle_ will have the opened object information.
 */
void
oapi_genobj::set_acl(struct ifs_security_descriptor *sd, 
    enum ifs_security_info sec_info, isi_error *&error)
{
	ASSERT(handle_ != NULL);

	iobj_genobj_acl_set(handle_, sd, sec_info, &error);
}

/*
 * This routine forwards the call to lower layer
 */
void
oapi_genobj::set_access_control(enum ostore_acl_mode acl_mode,
    int mode, uid_t uid, gid_t gid, isi_error *&error)
{
        ASSERT(handle_ != NULL);

	iobj_genobj_access_control_set(handle_, acl_mode, mode,
	    uid, gid, &error);
}

void
oapi_genobj::get_worm_attr(object_worm_attr &worm_attr,
    object_worm_domain &worm_dom, object_worm_domain *&worm_ancs,
    size_t &num_ancs, isi_error *&error)
{
	ASSERT(handle_ != 0);

	iobj_genobj_get_worm_attr(get_handle(), &worm_attr,
	    &worm_dom, &worm_ancs, &num_ancs, &error);
}

void oapi_genobj::get_protection_attr(struct pctl2_get_expattr_args *ge,
    isi_error *&error)
{
	ASSERT(handle_ != 0);

	iobj_genobj_get_protection_attr(get_handle(), ge, &error);
}


void oapi_genobj::set_protection_attr(struct pctl2_set_expattr_args *se, 
    isi_error *&error)
{
	ASSERT(handle_ != 0);

	iobj_genobj_set_protection_attr(get_handle(), se, &error);
}
