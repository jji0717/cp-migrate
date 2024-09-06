#ifndef __STORE_PROVIDER_H__
#define __STORE_PROVIDER_H__

#include <isi_rest_server/request.h>

#include "attribute_manager.h"
#include "oapi/oapi_store.h"
/**
 * This class encapsulates opening the appropriate stores
 * based on the input.
 *  
 */
class store_provider
{
public:
	/**
	 * Open the store based on the request.
	 * @param[IN/OUT] store -- the store to open
	 * @param[IN] input -- the request
	 * Throws exception when the store cannot be obtained
	 */
	static void open_store(oapi_store &store, const request &input);
	/**
	 * Open the store based on the store type name and uri.
	 * @param[IN/OUT] store  the store to open
	 * @param[IN] ns   the store type name
	 * @param[IN] uri  object uri
	 * Throws exception when the store cannot be obtained
	 */
	static void open_store(oapi_store &store, const std::string &ns,
	    const std::string &uri);

	/**
	 * Specialized function to open the two-tier object store
	 * Throws exception when the store cannot be obtained
	 */
	static oapi_store &open_ostore();

	static const attribute_manager &get_attribute_mgr(const request &input);
};

#endif

