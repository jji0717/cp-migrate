/**
 * @file
 *
 * S3 IO helper class for Cloud IO for CBM layer
 *
 * Copyright (c) 2012-2013
 * EMC Isilon Systems, Inc.  All rights reserved.
 *
 */

#include <algorithm>

#include <isi_ilog/ilog.h>

#include "s3_io_helper.h"
#include "isi_cbm_ioh_creator.h"

using namespace isi_cloud;

namespace {

/*
 * S3 providers have different retrable failure modes than other providers.
 * E.g., a failure with an AUTH_DEFAULT setting could be successful if auth_type
 * was explicitly pointed to an older version.  So, we decorate s3_provider
 * here to catch and handle these types of failures.  Note that this is done
 * here to allow the client (io_helper) to update its cl_account structure and
 * avoid unnecessarily retrying in the future.
 * we don't support V4->V2 fallback for C2S account as C2S account requries V4
 * authentications using temporary credential instead of static credential used
 * by other Non-C2S S3 accounts.
 */
#define S3_WRAP(stmt)								\
	if (acct.auth_type != AUTH_DEFAULT) {					\
		return stmt;							\
										\
	} else {								\
		try {								\
			return stmt;						\
		} catch(cl_exception &cl_ex) {					\
			if (owner->get_account().type == CPT_AWS ||             \
			    !cl_ex.might_be_authentication_type_failure())	\
				throw;						\
										\
			ilog(IL_DEBUG, "Function %s failed at with "		\
			    "AUTH_DEFAULT (reason %s), trying AUTH_AWS_V2",     \
			     __FUNCTION__, cl_ex.what());	                \
			owner->get_account().auth_type = AUTH_AWS_V2;		\
			return stmt;						\
		}								\
	}

class s3_provider_wrap : public isi_cloud::cl_provider {
public:
	s3_provider_wrap(isi_cloud::cl_provider *impl, s3_io_helper *caller)
	    : s3_p_(impl), owner(caller)
	{
		ASSERT(s3_p_ != NULL, "Need non-NULL s3_p_ parameter");
		ASSERT(owner != NULL, "Need non-NULL caller parameter");
	};

	void put_container(const cl_account &acct, const std::string &container,
	   callback_params &cb_params)
	{
		S3_WRAP(
		    s3_p_->put_container(acct, container, cb_params);
		);
	};

	std::string get_container_location(const cl_account &acct,
	    const std::string &container,
	    callback_params &cb_params)
	{
		S3_WRAP(
		    s3_p_->get_container_location(acct, container, cb_params);
		);
	};

	void put_object(const cl_account &acct, const std::string &container,
	    const std::string &object_id, const str_map_t &attr_map,
	    size_t length, bool overwrite, bool random_io,
	    const std::string &content_md5, callback_params &cb_params,
	    idata_stream *istrm, int new_container_levels)
	{
		S3_WRAP(
		    s3_p_->put_object(acct, container, object_id, attr_map,
			length, overwrite, random_io, content_md5, cb_params,
			istrm, new_container_levels);
		);
	};

	void update_object(const cl_account &acct, const std::string &container,
	    const std::string &object_id, off_t offset, size_t length,
	    callback_params &cb_params, idata_stream *istrm)
	{
		S3_WRAP(
		    s3_p_->update_object(acct, container, object_id, offset,
			length, cb_params, istrm);
		);
	};

	void get_object(const cl_account &acct, const std::string &container,
	    const std::string &object_id, off_t offset, size_t length,
	    uint64_t snapid, str_map_t &attr_map, struct cl_properties &prop,
	    callback_params &cb_params, odata_stream &ostrm)
	{
		S3_WRAP(
		    s3_p_->get_object(acct, container, object_id, offset, length,
			snapid, attr_map, prop, cb_params, ostrm);
		);
	};

	void head_container(const cl_account &acct,
	    const std::string &container, callback_params &cb_params)
	{
		S3_WRAP(
		    s3_p_->head_container(acct, container, cb_params);
		);
	};

	void head_object(const cl_account &acct, const std::string &container,
	    const std::string &object_id, str_map_t &attr_map,
	    callback_params &cb_params)
	{
		S3_WRAP(
		    s3_p_->head_object(acct, container, object_id, attr_map,
			cb_params);
		);
	};

	void snapshot_object(const cl_account &acct,
	    const std::string &container, const std::string &object,
	    uint64_t &snapshot_id, callback_params &cb_params)
	{
		S3_WRAP(
		    s3_p_->snapshot_object(acct, container, object, snapshot_id,
			cb_params);
		);
	};

	void clone_object(const cl_account &acct,
	    const std::string &dst_container, const std::string &dst_object_id,
	    const std::string &src_container, const std::string &src_object_id,
	    bool overwrite, const std::string &snapshot_name,
	    callback_params &cb_params, int new_container_levels)
	{
		S3_WRAP(
		    s3_p_->clone_object(acct, dst_container, dst_object_id,
			src_container, src_object_id, overwrite, snapshot_name,
			cb_params, new_container_levels);
		);
	};

	void move_object(const cl_account &acct,
	    const std::string &dst_container, const std::string &dst_object_id,
	    const std::string &src_container, const std::string &src_object_id,
	    bool overwrite, callback_params &cb_params,
	    int new_container_levels)
	{
		S3_WRAP(
		    s3_p_->move_object(acct, dst_container, dst_object_id,
			src_container, src_object_id, overwrite, cb_params,
			new_container_levels);
		);
	};

	void delete_object(const cl_account &acct, const std::string &container,
	    const std::string &object_id, uint64_t snapid,
	    callback_params &cb_params)
	{
		S3_WRAP(
		    s3_p_->delete_object(acct, container, object_id, snapid,
			cb_params);
		);
	};

	void get_account_cap(const cl_account &acct, cl_account_cap &accnt_cap,
	    callback_params &cb_params)
	{
		S3_WRAP(
		    s3_p_->get_account_cap(acct, accnt_cap, cb_params);
		);
	};

	void list_object(const cl_account &acct, const std::string &container,
	    cl_list_object_param &list_obj_param, callback_params &cb_params)
	{
		S3_WRAP(
		    s3_p_->list_object(acct, container, list_obj_param,
			cb_params);
		);
	};

	void list_container(const cl_account &acct,
	    cl_list_object_param &list_obj_param, callback_params &cb_params)
	{

	};

	void get_account_statistics(cl_account &acct,
	    const cl_account_statistics_param &acct_stats_param,
	    cl_account_statistics &acct_stats)
	{
		S3_WRAP(
		    s3_p_->get_account_statistics(acct, acct_stats_param,
			acct_stats);
		);
	};

private:
	isi_cloud::cl_provider *s3_p_;
	s3_io_helper *owner;
};

/**
 * Routine to create an instance of s3 io helper
 * @param acct[in] s3 account
 * @return pointer to the created instance
 */
isi_cbm_ioh_base *
create_s3_io_helper(const isi_cbm_account &acct)
{
	return new s3_io_helper(acct);
}

/**
 * Register object that will register creation function to io helper
 * factory upon module loading
 */
isi_cbm_ioh_register g_reg(CPT_AWS, create_s3_io_helper);

}


s3_io_helper::s3_io_helper(const isi_cbm_account &acct)
	: isi_cbm_ioh_base(acct)
{
	// Only Google, ECS and AWS use this class
	ASSERT(acct.type == CPT_AWS ||
	    acct.type == CPT_ECS ||
	    acct.type == CPT_ECS2 ||
	    acct.type == CPT_GOOGLE_XML,
	    "Only S3, ECS, ECS2 and GOOGLE_XML should use s3_io_helper "
		"(%d requested)", acct.type);

	s3_p_ = new s3_provider_wrap(
	    &cl_provider_creator::get_provider(acct.type), this);
	// no need to free s3 provider pointer
	// s3_p_ = dynamic_cast<s3_provider *>(&prod);
	// ASSERT(s3_p_);
}

s3_io_helper::~s3_io_helper()
{
	delete s3_p_;
}

void
s3_io_helper::match_cmo_attr(const str_map_t *cmo_attr_to_match,
    const cl_object_name &cloud_name)
{
	str_map_t cmo_attr;
	str_map_t::const_iterator itr;

	if (!cmo_attr_to_match || cmo_attr_to_match->size() == 0)
		return;

	get_cmo_attrs(cloud_name, cmo_attr);
	itr = cmo_attr_to_match->begin();

	for (; itr != cmo_attr_to_match->end(); ++itr) {
		std::string meta_str = itr->first;

		std::transform(meta_str.begin(), meta_str.end(),
		    meta_str.begin(), ::tolower);

		if (cmo_attr[meta_str] == itr->second)
			continue;

		// mismatched value if reaching here
		std::string msg(itr->first);
		msg.append("'s value mismatched with CMO");

		throw isi_cbm_exception(CBM_IOH_MISMATCHED_CMO_ATTR, msg);
	}
}
