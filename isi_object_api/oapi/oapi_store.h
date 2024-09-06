#ifndef __OAPI_STORE_H__
#define __OAPI_STORE_H__

#include <memory>
#include <pthread.h>
#include <string>
#include <set>

#include <isi_object/ostore.h>

class oapi_store {

public:	
	
	/**
	 * Get the object store handle.
	 */
	ostore_handle *get_handle() const { return store_handle_; }

	/**
	 * Get the object store handle.
	 */
	OSTORE_TYPE get_store_type() const { return store_type_; }

	/**
	 * get the store name
	 */
	const std::string &get_name() const { return store_name_; }

	/**
	 * Open store a store.
	 * @param[IN] store_type the store type
	 * @param[IN] store the store name
	 * @param[IN] ns_path the namespace path -- in the case of object, must be
	 * NULL, in the case of namespace, the full path of access point.
	 * @param error: the error of if any
	 */
	void create_store(OSTORE_TYPE store_type,
	    const std::string &store,
	    const char *ns_path,
	    isi_error *&error);

	/**
	 * Open store a store.
	 * @param[IN] store_type the store type
	 * @param[IN] store the store name
	 * @param[IN] error the error if any
	 */
	void open_store(OSTORE_TYPE store_type,
	    const std::string &store,
	    isi_error *&error);

	/**
	 * Delete the store.
	 *
	 * This oapi_store object becomes invalid after deletion.
	 * @pre the store must be opened or created
	 *
	 * @param[INOUT] error the error if any
	 */
	void destroy_store(isi_error *&error);

	/**
	 * Close the store.
	 *
	 * This oapi_store object becomes invalid after success close.
	 *
	 * @param[IN] suppress  if true, no error returned to caller
	 * @return isi_error object if any error
	 */
	isi_error *close(bool suppress);

public:

	oapi_store() : store_handle_(NULL), store_type_(OST_INVALID) {}
	~oapi_store();

private:
	oapi_store(const oapi_store &);
	oapi_store &operator=(const oapi_store &);

	ostore_handle *store_handle_;
	OSTORE_TYPE store_type_;
	std::string store_name_;
};

#endif //__OAPI_STORE_H__
