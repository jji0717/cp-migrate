#ifndef __OAPI_OBJECT_H__
#define __OAPI_OBJECT_H__

#include "oapi_bucket.h"
#include "oapi_attrval.h"
#include <time.h>
#include <map>
#include <string>

#include "oapi_genobj.h"

/**
 * Thin wrapper of the object library object.
 */
class oapi_object : public oapi_genobj {

public:
	oapi_object(const oapi_store &store) : oapi_genobj(store, NULL, ""),
	    bucket_(store), created_(false) {}

	oapi_object(const oapi_store &store, igenobj_handle *handle,
	    const char *path) : oapi_genobj(store, handle, path),
	    bucket_(store), created_(false) {}
	    	
	~oapi_object();

	/**
	 * Create an object with the given name.
	 * @param[IN] acct_name - the account name
	 * @param[IN] bucket_name - the bucket name
	 * @param[IN] object_name - the object name
	 * @param[IN] c_mode - permission for the object
	 * @param[IN] acl_mode - ACL permission for the object.
	 * @param[IN] overwrite - indicates if the object can be replaced or
	 *                        not if it already exists.
	 * @param[OUT] error - the error on failure
	 */
	void create(const char *acct_name,
	    const char *bucket_name, 
	    enum ifs_ace_rights bucket_open_ace_mode,
	    const char *object_name,
	    mode_t c_mode, enum ostore_acl_mode acl_mode, 
	    bool overwrite, isi_error *&error);

	/**
	 * Create an object with the given name under the given bucket.
	 * @param[IN] bucket - the bucket
	 * @param[IN] object_name - the object name
	 * @param[IN] c_mode - permission for the object
	 * @param[IN] acl_mode - ACL permission for the object.
	 * @param[IN] overwrite - indicates if the object can be replaced or
	 *                        not if it already exists.
	 * @param[OUT] error - the error on failure
	 */
	void create(const oapi_bucket &bucket,
	    const char *object_name, mode_t c_mode, 
	    enum ostore_acl_mode acl_mode, bool overwrite, 
	    isi_error *&error);

	/**
	 * Open an object with the given name.
	 * @param[IN] acct_name - the account name
	 * @param[IN] bucket_name - the bucket name
	 * @param[IN] object_name - the object name
	 * @param[OUT] error - the error on failure
	 */
	void open(const char *acct_name,
	    const char *bucket_name,
	    enum ifs_ace_rights bucket_open_ace_mode,
	    const char *object_name, 
	    enum ifs_ace_rights obj_open_ace_mode,
	    int flags, struct ifs_createfile_flags cf_flags,
	    isi_error *&error);

	/**
	 * Delete an object with the given name.
	 * @param[IN] acct_name - the account name
	 * @param[IN] bucket_name - the bucket name
	 * @param[IN] object_name - the object name
	 * @param[OUT] error - the error on failure
	 */
	void remove(const char *acct_name,
	    const char *bucket_name,
	    const char *object_name, isi_error *&error);
	
	/**
	 * Write data to the object at the offset for the len given.
	 * @param[IN] data - the data to be written
	 * @param[IN] offset - the offset of the object data stream
	 * @param[IN] len - the length in bytes of the data to write
	 * @param[OUT] error - the error on failure
	 */
	void write(const void *data, off_t offset, size_t len,
	    isi_error *&error);

	/**
	 * Read data from the object at the offset for the len given.
	 * @param[OUT] data - the buffer to load the data
	 * @param[IN] offset - the offset of the object data stream
	 * @param[IN] buff_len - the length in bytes of the data buffer
	 * @param[OUT] read_len - the length of data read in bytes
	 * @param[OUT] error - the error on failure
	 */
	void read(void *data, off_t offset, size_t buff_len,
	    size_t &read_len, isi_error *&error);

	/**
	 * Set the attribute of the object with a name "key"
	 * and value "value".
	 * @param[IN] key - the key name
	 * @param[IN] value - the value of the attribute
	 * @param[IN] value_len - the length in bytes of the value
	 * @param[OUT] error - the error on failure
	 */
	void set_attr(const char *key, const void *value, size_t value_len,
	    isi_error *&error);

	/**
	 * Get the value of attribute of the object
	 * @param[IN] key - the key name
	 * @param[OUT] attr_value - the value of the attribute
	 * @param[OUT] error - the error on failure
	 * @return true if attribute found and success, false on failure
	 *         or the key not found
	 */
	bool get_attr(const char *key, oapi_attrval &attr_val,
	   isi_error *&error);

	/**
	 * Delete the attribute of the object with a name "key"
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
	 * Get system attributes of the object such as, size, modification time
	 * @param[OUT] sys_attr - system attribute
	 * @param[OUT] error - the error on failure
	 */
	void get_sys_attr(object_sys_attr &sys_attr, isi_error *&error);

	/**
	 *
	 */
	void set_worm_attr(const object_worm_set_arg &set_arg,
	    isi_error *&error);

	/**
	 * Return the object handle
	 */
	iobject_handle *get_handle() const {return (iobject_handle *)handle_;}

	/**
	 * Commit the object creation
	 */
	void commit(isi_error *&error);

	/**
	 * close the opened object
	 */
	void close();

private:
	oapi_bucket bucket_;
	// not implemented:
	oapi_object(const oapi_object &other);
	oapi_object & operator= (const oapi_object &other);
	bool created_;
	std::string name_;
};

#endif //__OAPI_OBJECT_H__

