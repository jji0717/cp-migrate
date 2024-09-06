#ifndef __OAPI_BUCKET_H__
#define __OAPI_BUCKET_H__

#include "oapi_store.h"
#include "oapi_genobj.h"
#include "isi_object_api/handler_common.h"
#include <isi_acl/isi_sd.h>
#include <isi_rest_server/api_utils.h>

/**
 * Thin wrapper of the bucket object
 */
class oapi_bucket : public oapi_genobj {

public:	
	oapi_bucket(const oapi_store &store) : oapi_genobj(store, NULL, "") {}
	oapi_bucket(oapi_store &store, igenobj_handle *handle, const char *path)
	    : oapi_genobj(store, handle, path) {}
		
	~oapi_bucket();	

	/**
	 * Create a bucket with the given name.
	 * @praram[IN] bucket_name - the bucket name
	 * @param[IN] create_mode - the mode used to create the bucket
	 * @param[IN] attrs - attributes for the bucket
	 * @param[IN] prot - protection property for the bucket
	 * @param[IN] acl_mode - ACL permission to set for the bucket.
	 * @param[OUT] error - the error on failure
	 */
	void create(const char *acct_name, const char *bucket_name,
	    mode_t create_mode, const struct iobj_object_attrs *attrs,
	    const struct iobj_set_protection_args *prot, bool recursive,
	    enum ostore_acl_mode acl_mode, bool overwrite,
	    isi_error *&error);

	/**
	 * Open a bucket with the given name.
	 * @praram[IN] bucket_name - the bucket name
	 * @param[OUT] error - the error on failure
	 */
	void open(const char *acct_name, const char *bucket_name, 
	    enum ifs_ace_rights ace_mode, int flags, 
	    struct ifs_createfile_flags cf_flags, isi_error *&error);

	/**
	 * Delete a bucket with the given name.
	 * @praram[IN] bucket_name - the bucket name
	 * @param[OUT] error - the error on failure
	 */
	void remove(const char *acct_name,
	    const char *bucket_name, isi_error *&error);

	/**
	 * Set the attribute of the bucket with a name "key"
	 * and value "value".
	 * @param[IN] key - the key name
	 * @param[IN] value - the value of the attribute
	 * @param[IN] value_len - the length in bytes of the value
	 * @param[OUT] error - the error on failure
	 */
	void set_attr(const char *key, const void *value, size_t value_len,
	    isi_error *&error);

	/**
	 * Get the value of attribute of the bucket
	 * @param[IN] key - the key name
	 * @param[OUT] attr_value - the value of the attribute
	 * @param[OUT] error - the error on failure
	 * @return true if attribute found and success, false on failure
	 *         or the key not found
	 */
	bool get_attr(const char *key, oapi_attrval &attr_val,
	   isi_error *&error);

	/**
	 * Delete the attribute of the bucket with a name "key"
	 * @param[IN] key - the key name
	 * @param[OUT] error - the error on failure
	 */
	void delete_attr(const char *key, isi_error *&error);

	/**
	 * List attributes
	 * @param[IN] attrs - attribute pairs
	 * @param[OUT] error - the error on failure
	 */
	void list_attrs(std::map<std::string, std::string> &attrs,
	    isi_error *&error);

	/**
	 * Get system attributes of the bucket such as, size, modification time
	 * @param[OUT] sys_attr - system attribute
	 * @param[OUT] error - the error on failure
	 */
	void get_sys_attr(object_sys_attr &sys_attr, isi_error *&error);

	/**
	 * close the opened bucket
	 */
	void close();

	ibucket_handle *get_handle() const 
	{
		return (ibucket_handle *)handle_;
	}

private:

	//ibucket_handle *bucket_handle_;

	// not implemented:
	oapi_bucket(const oapi_bucket &other);
	oapi_bucket & operator= (const oapi_bucket &other);
};

#endif //__OAPI_BUCKET_H__

