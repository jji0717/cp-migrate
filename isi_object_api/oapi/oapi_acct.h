#ifndef __OAPI_ACCT_H__
#define __OAPI_ACCT_H__

#include "oapi_store.h"
#include <string>

extern "C" {

typedef bool (*LIST_BUCKET_CALLBACK) (void * context, const char * name );

}

/**
 * Thin wrapper of the account object
 */
class oapi_acct {

public:
	oapi_acct(const std::string &acct) : acct_(acct) {}

	~oapi_acct();

	/**
	 * List the buckets
	 */
	void list_bucket(oapi_store &store, LIST_BUCKET_CALLBACK callback,
	    void *context, isi_error *&error);


	const std::string & acct() const {return acct_;}

private:
	std::string acct_;

	// not implemented:
	oapi_acct(const oapi_acct & other);
	oapi_acct & operator= (const oapi_acct & other);
};

#endif //__OAPI_BUCKET_H__
