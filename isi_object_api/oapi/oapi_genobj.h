#ifndef __OAPI_GENOBJ_H__
#define __OAPI_GENOBJ_H__

#include "oapi_store.h"

#include <errno.h>
#include <map>
#include <string>
#include <isi_ilog/ilog.h>
#include <isi_util_cpp/isi_exception.h>
#include <isi_acl/isi_sd.h>

#include "oapi_attrval.h"

class oapi_genobj {
public:

	struct object_sys_attr {
		struct stat attr_;
		uint64_t objrev;
	};

	/**
	 * Return the object handle
	 */
	igenobj_handle *get_handle() const {return handle_;}

	/**
	 * close the opened object
	 */
	virtual void close();

	/**
	 * Get the object type
	 */
	OSTORE_OBJECT_TYPE get_object_type();

	/**
	 * Get the object name
	 */
	const char *get_name();

	/**
	 * Set the attribute of the object with a name "key"
	 * and value "value".
	 * @param[IN] key - the key name
	 * @param[IN] value - the value of the attribute
	 * @param[IN] value_len - the length in bytes of the value
	 * @param[OUT] error - the error on failure
	 */
	virtual void set_attr(const char *key, const void *value,
	    size_t value_len, isi_error *&error) {}

	/**
	 * Get the value of attribute of the object
	 * @param[IN] key - the key name
	 * @param[OUT] attr_value - the value of the attribute
	 * @param[OUT] error - the error on failure
	 * @return true if attribute found and success, false on failure
	 *         or the key not found
	 */
	virtual bool get_attr(const char *key, oapi_attrval &attr_val,
	    isi_error *&error) {return false;}

	/**
	 * Delete the attribute of the object with a name "key"
	 * @param[IN] key - the key name
	 * @param[OUT] error - the error on failure
	 */
	virtual void delete_attr(const char *key, isi_error *&error) {}

	/**
	 * List attributes
	 * @param[IN] attrs - attribute pairs
	 * @param[OUT] error - the error on failure
	 */
	virtual void list_attrs(std::map<std::string, std::string> &attrs,
	    isi_error *&error) {}

	/**
	 * Get system attributes of the object such as, size, modification time
	 * @param[OUT] sys_attr - system attribute
	 * @param[OUT] error - the error on failure
	 */
	virtual void get_sys_attr(object_sys_attr &sys_attr,
	    isi_error *&error) {}

	/**
	 * Get Worm attributes for this object, if applicable.  The output
	 * structs will be empty if the object does not support those worm
	 * attributes.
	 * @param[OUT] worm_attr - the worm file attributes
	 * @param[OUT] worm_dom - the worm domain attributes
	 */
	virtual void get_worm_attr(object_worm_attr &worm_attr,
	    object_worm_domain &worm_dom, object_worm_domain *&worm_ancs,
	    size_t &num_ancs, isi_error *&error);


	/**
	 * Get Protection attributes for this object
	  */
	virtual void get_protection_attr(struct pctl2_get_expattr_args *ge,
	    isi_error *&error);

	/**
	 * Set Protection attributes for this object
	  */
	virtual void set_protection_attr(struct pctl2_set_expattr_args *se, 
		isi_error *&error);

	/**
	 * Delete the object
	 *
	 * After deletion, the object becomes invalid
	 * @param[IN]  path  - path of the file/directory to be removed.
	 * @param[IN]  flags - specifies deleting options
	 * @param[OUT] error - the error on failure
	 */
	void remove(const char * path, ostore_del_flags flags, 
	    isi_error *&error);

	/**
	 * Copy file object or directory
	 *
	 * The errors during copy are reported via user provided callback.
	 * A single isi_error object may be generated as output @a error.
	 *
	 * @param[IN] acct_name - the account name
	 * @param[IN] cp_param - parameters needed to execute copy
	 * @param[OUT] error - isi_error object
	 * @return 0 if successful; otherwise non-0.
	 */
	static int copy(const char *acct_name,
	    struct ostore_copy_param *cp_param, isi_error *&error);

	/**
	 * Move file or directory
	 *
	 * @param src_ios The source store handle
	 * @param src The source object path
	 * @param dest_ios The target store handle
	 * @param dest The target object path
	 * @param[OUT] err_out - isi_error object if failed to move
	 *
	 * @return 0 if successful; 1 if found source not exists or destination
	 *         exists before starting to move; or -1 otherwise
	 */
	static int move(const char *acct_name, struct ostore_handle *src_ios,
	    const char *src, struct ostore_handle *dest_ios,
	    const char *dest, struct fsu_move_flags *mv_flags,
	    isi_error *&error);

	bool is_open() const { return handle_ != NULL; }

	/**
	 * Get the logical path of the object
	 */
	const std::string &get_path() const { return path_; }

	/**
	 * Get the store	 *
	 */
	const oapi_store &get_store() const { return store_; }

	/**
	 * Get acl of the bucket/directory/object/file.
	 * @param[IN] sec_info security information. 
	 * @param[OUT] sd_out - security descriptor information.
	 * @param[OUT] error - the error on failure
	 */	
	void get_acl(enum ifs_security_info sec_info, 
	    ifs_security_descriptor **sd_out, isi_error *&error);

	/**
	 * Get acl of the bucket/directory/object/file.
	 * @param[IN] mods acl modifications. 
	 * @param[IN] num_mods - number of modifications.
	 * @param[IN] acl_mod_type - indicates replace or modify operation 
	 *                on ace's in the acl.
	 * @param[OUT] sd_out - final security descriptor.
	 * @param[OUT] error - the error on failure
	 */	

	void modify_sd(struct ifs_ace_mod *mods, int num_mods, 
	    enum ifs_acl_mod_type acl_mod_type,
	    enum ifs_security_info sec_info,
	    struct ifs_security_descriptor **sd_out, 
	    isi_error *&error);

	/**
	 * Set acl of the bucket/directory/object/file.
	 * @param[IN] sd - security descriptor information.
	 * @param[IN] sec_info - security information. 
	 * @param[OUT] error - the error on failure
	 */	
	void set_acl(struct ifs_security_descriptor *sd, 
	    enum ifs_security_info sec_info, isi_error *&error);

	/**
	 * Set acl or unix permission mode and ownership for the object.
	 *
	 * The mode is used only when @acl_mode is OSTORE_ACL_NONE
	 *
	 * If mode or uid or gid has value of -1, then corresponding
	 * unix permission mode or owner user id or group id will not
	 * be changed.
	 *
	 * @param[IN] acl_mode - predefined ACL mode
	 * @param[IN] mode - unix permission mode
	 * @param[IN] uid - owner user id
	 * @param[IN] gid - group id
	 * @param[OUT] error - the error on failure
	 */
	void set_access_control(enum ostore_acl_mode acl_mode,
	    int mode, uid_t uid, gid_t gid, isi_error *&error);

	/**
	 * Get owner of the object entity using the file descriptor.
	 * @param[IN] fd - file descriptor of the entity.
	 * @param[OUT] owner - Owner of the entity (Directory/File).
	 */	
	void get_sd_owner(int fd, char *name, struct persona **owner, 
	    isi_error *&error);

	/**
	 * Compare the personas passed.
	 * @param[IN] p1 - Persona 1.
	 * @param[IN] p2 - Persona 2.
	 * @param[OUT] bool - true if the persona matches, false otherwise.
	 */	
	bool compare_persona(struct persona *p1, struct persona *p2);

	oapi_genobj(const oapi_store &store, igenobj_handle *handle,
	    const char *path) : handle_(handle), path_(path ? path : ""),
	    store_(store) {}

protected:
	void set_path(const std::string &path) { path_ = path; }

public:
	virtual ~oapi_genobj();
	
protected:
	igenobj_handle *handle_;
private:
	std::string path_; // The logical path of the object
	const oapi_store &store_;
};

#endif
