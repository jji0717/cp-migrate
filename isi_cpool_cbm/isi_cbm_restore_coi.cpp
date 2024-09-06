#include "isi_cbm_restore_coi.h"

#include <boost/utility.hpp>
#include <isi_cloud_common/isi_cpool_version.h>
#include <isi_cloud_api/isi_cloud_api.h>
#include <isi_config/ifsconfig.h>
#include <isi_cpool_d_common/isi_tri_bool.h>

#include "isi_cbm_account_util.h"
#include "isi_cbm_id.h"
#include "isi_cbm_error_util.h"
#include "isi_cbm_file.h"
#include "isi_cbm_read.h"
#include "isi_cbm_scoped_ppi.h"
#include "isi_cbm_scoped_coid_lock.h"

namespace
{

	const unsigned int RSTR_MAGIC = 0x52535452; // 'RSTR'

	/**
	 * maximum 256 bytes -- we use much less
	 */
	const unsigned int RSTR_MAX_TOKEN_SZ = 256;

	/**
	 * on-disk structure for the restore log
	 */
	struct restore_log_header
	{
		/**
		 * the magic number of the restore log
		 */
		unsigned int magic_;
		/**
		 * the version number for the restore log format
		 */
		unsigned int version_;

		/**
		 * The following two are for informational purpose only
		 */
		char cluster_id_[IFSCONFIG_GUID_SIZE * 2];
		uint32_t acct_id_;

		char token_[RSTR_MAX_TOKEN_SZ];
		uint64_t count_; // count of object processed by this restorer
	} __packed;

	const size_t RSTR_LOG_SZ = sizeof(restore_log_header);

	class cbm_coi_restorer : boost::noncopyable
	{
	public:
		/**
		 * constructor
		 */
		cbm_coi_restorer(const std::string &cluster_id, uint32_t acct_id,
						 struct timeval dod, void *ckpt_ctx, get_ckpt_data_func_t get_ckpt_cb,
						 set_ckpt_data_func_t set_ckpt_cb);

		/**
		 * restore a particular object
		 */
		bool restore_object(const struct cl_list_object_info &obj);

		/**
		 * restore objects in the accounts
		 */
		void restore_objects(struct isi_error **error_out);

		/**
		 * accessors below:
		 */
		isi_cbm_account &account() { return account_; }
		isi_cbm_ioh_base_sptr &io_helper() { return io_helper_; }
		void *&ckpt_ctx() { return ckpt_ctx_; }
		get_ckpt_data_func_t &get_ckpt_cb() { return get_ckpt_cb_; }
		set_ckpt_data_func_t &set_ckpt_cb() { return set_ckpt_cb_; }
		struct timeval &dod() { return dod_; }

	private:
		std::string cluster_id_;
		uint32_t acct_id_;
		struct timeval dod_;
		void *ckpt_ctx_;
		get_ckpt_data_func_t get_ckpt_cb_;
		set_ckpt_data_func_t set_ckpt_cb_;
		struct isi_cbm_account account_;
		isi_cbm_ioh_base_sptr io_helper_;
		cl_list_object_param param_;
		uint64_t count_;
	};

	cbm_coi_restorer::cbm_coi_restorer(const std::string &cluster_id,
									   uint32_t acct_id, struct timeval dod, void *ckpt_ctx,
									   get_ckpt_data_func_t get_ckpt_cb, set_ckpt_data_func_t set_ckpt_cb)
		: cluster_id_(cluster_id), acct_id_(acct_id), dod_(dod),
		  ckpt_ctx_(ckpt_ctx), get_ckpt_cb_(get_ckpt_cb), set_ckpt_cb_(set_ckpt_cb),
		  count_(0)
	{
	}

	bool ///////list object之后,执行回调函数
	cbm_coi_restorer::restore_object(const struct cl_list_object_info &obj)
	{
		isi_error *error = NULL;
		bool to_continue = true;
		isi_cbm_coi_sptr coi;
		struct cl_object_name cloud_object_name;
		isi_cloud_object_id object_id;
		isi_cbm_ioh_base *ioh = io_helper_.get();

		try
		{
			ioh->get_object_id_from_cmo_name(obj.name, obj.snapid,
											 object_id);
		}
		catch (isi_cbm_exception &ex)
		{
			// ignore any errors from the cloud objects
			ilog(IL_NOTICE, "error code: %d msg: %s for obj %s %ld",
				 ex.get_error_code(), ex.what(), obj.name.c_str(),
				 obj.snapid);
			goto out;
		}

		cloud_object_name.container_cmo = account_.container_cmo;
		cloud_object_name.container_cdo = "unused";
		cloud_object_name.obj_base_name = object_id;

		ilog(IL_DEBUG, "count: %ld restore object %s\n", ++count_,
			 obj.name.c_str());
		coi = isi_cbm_get_coi(&error);
		ON_ISI_ERROR_GOTO(out, error);

		{
			uint8_t node_id[IFSCONFIG_GUID_SIZE] = {0};
			account_.account_id.get_cluster_id(node_id);
			scoped_coid_lock coid_lock(*coi.get(), object_id);
			coid_lock.lock(true, &error);
			ON_ISI_ERROR_GOTO(out, error);
			coi->restore_object(object_id,
								(uint8_t *)node_id, account_.account_id,
								account_.container_cmo.c_str(),
								TB_UNKNOWN,
								TB_UNKNOWN,
								dod_, &error);
			ON_ISI_ERROR_GOTO(out, error);
		}

		// Write check pointing information if any
		if (set_ckpt_cb_ && param_.cookies_out.size() > 0)
		{
			// successfully updated COI for this object, mark it in the
			// checkpoint info
			djob_op_job_task_state t_state = DJOB_OJT_NONE;
			restore_log_header header;
			memset(&header, 0, sizeof(header));
			header.magic_ = RSTR_MAGIC;
			header.version_ = CPOOL_RSTR_VERSION;

			ASSERT(cluster_id_.size() <= sizeof(header.cluster_id_));
			memcpy(header.cluster_id_, cluster_id_.c_str(),
				   cluster_id_.size());
			header.acct_id_ = acct_id_;

			// Save the cookie, limiting the sizes to what we can
			// handle.
			std::vector<std::string>::const_iterator it =
				param_.cookies_out.begin();
			size_t avail_len = sizeof(header.token_);
			char *buff = header.token_;
			for (; it != param_.cookies_out.end() && avail_len > 0; ++it)
			{
				const std::string &cookie = *it;
				size_t len_to_cp = MIN(avail_len, cookie.size());
				memcpy(buff, cookie.c_str(), len_to_cp);
				avail_len -= len_to_cp;
				buff += len_to_cp;
			}

			header.count_ = count_;
			bool ok = set_ckpt_cb_(ckpt_ctx_,
								   &header, RSTR_LOG_SZ, &t_state, 0);

			if (!ok)
			{
				ilog(IL_ERR, "Failed to set checkpoint error: %#{} for "
							 "%s",
					 isi_error_fmt(error), obj.name.c_str());
				to_continue = false;
				goto out;
			}
			if (t_state == DJOB_OJT_PAUSED)
			{
				ilog(IL_NOTICE, "Restore paused for (%s %d)",
					 cluster_id_.c_str(), acct_id_);
				to_continue = false;
				goto out;
			}
			else if (t_state == DJOB_OJT_CANCELLED)
			{
				ilog(IL_NOTICE, "Restore canceled for (%s %d)",
					 cluster_id_.c_str(), acct_id_);
				to_continue = false;
				goto out;
			}
		}

	out:
		if (error)
		{
			ilog(IL_ERR, "Error when restoring COI error: %#{} for "
						 "%s",
				 isi_error_fmt(error), obj.name.c_str());
			to_continue = false;
		}
		return to_continue;
	}

	bool
	cbm_restore_coi_list_object_cb(void *caller_context,
								   const struct cl_list_object_info &obj)
	{
		ASSERT(caller_context != NULL);
		cbm_coi_restorer *restorer =
			(cbm_coi_restorer *)caller_context;

		return restorer->restore_object(obj);
	}

	void
	cbm_coi_restorer::restore_objects(struct isi_error **error_out)
	{
		isi_error *error = NULL;
		isi_cbm_id cbm_acct_id(acct_id_, cluster_id_.c_str());
		void *ckpt_data = NULL;
		djob_op_job_task_state t_state = DJOB_OJT_NONE;

		cpool_events_attrs cev_attrs;
		{
			scoped_ppi_reader ppi_reader;

			struct isi_cfm_policy_provider *ppi;

			ppi = (struct isi_cfm_policy_provider *)
					  ppi_reader.get_ppi(&error);
			ON_ISI_ERROR_GOTO(out, error, "failed to get ppi for (%s %d)",
							  cluster_id_.c_str(), acct_id_);

			isi_cfm_get_account((isi_cfm_policy_provider *)ppi,
								cbm_acct_id, account(), &error);
			ON_ISI_ERROR_GOTO(out, error, "failed to "
										  "isi_cfm_get_account for (%s %d)",
							  cluster_id_.c_str(),
							  acct_id_);
		}

		io_helper() = isi_cpool_get_io_helper(account(), &error);

		ON_ISI_ERROR_GOTO(out, error, "failed to "
									  "isi_cpool_get_io_helper for (%s %d)",
						  cluster_id_.c_str(),
						  acct_id_);

		// load the resume information
		if (get_ckpt_cb_)
		{
			size_t header_size = RSTR_LOG_SZ;
			restore_log_header *header = NULL;
			bool ok = get_ckpt_cb_(ckpt_ctx_, &ckpt_data, &header_size,
								   &t_state, 0);
			if (ckpt_data == NULL)
			{
				ilog(IL_DEBUG, "Restoring (%s %d) has no checkpointing "
							   "information.",
					 cluster_id_.c_str(), acct_id_);
				goto perform_restore;
			}
			if (header_size != RSTR_LOG_SZ)
			{
				ilog(IL_NOTICE, "Restoring (%s %d)has wrong "
								"checkpointing information, size: %ld, "
								"expected: %ld.",
					 cluster_id_.c_str(), acct_id_, header_size,
					 RSTR_LOG_SZ);
				ok = false;
				goto perform_restore;
			}

			if (t_state == DJOB_OJT_PAUSED)
			{
				error = isi_cbm_error_new(CBM_FILE_PAUSED,
										  "Restore paused for (%s %d)",
										  cluster_id_.c_str(), acct_id_);
				goto out;
			}
			else if (t_state == DJOB_OJT_CANCELLED)
			{
				error = isi_cbm_error_new(CBM_FILE_CANCELLED,
										  "Restore canceled for (%s %d)",
										  cluster_id_.c_str(), acct_id_);
				goto out;
			}
			else if (t_state == DJOB_OJT_STOPPED)
			{
				error = isi_cbm_error_new(CBM_FILE_STOPPED,
										  "Restore stopped for (%s %d)",
										  cluster_id_.c_str(), acct_id_);
				goto out;
			}

			header = (restore_log_header *)ckpt_data;
			if (header->magic_ != RSTR_MAGIC)
			{
				ilog(IL_NOTICE, "Restoring (%s %d) has wrong "
								"checkpointing header information, magic: %x, "
								"expected: %x.",
					 cluster_id_.c_str(), acct_id_, header->magic_,
					 RSTR_MAGIC);
				ok = false;
				goto perform_restore;
			}

			if (CPOOL_RSTR_VERSION != header->version_)
			{
				ilog(IL_NOTICE, "Restoring (%s %d) has wrong "
								"checkpointing header "
								"information, version: %x, expected: %x.",
					 cluster_id_.c_str(), acct_id_, header->version_,
					 CPOOL_RSTR_VERSION);
				ok = false;
				goto perform_restore;
			}
			count_ = header->count_;
			std::string token = header->token_;
			param_.cookies_in.push_back(token);
		}

	perform_restore:
		param_.caller_cb = cbm_restore_coi_list_object_cb;
		param_.caller_context = this;
		param_.attribute_requested = true;
		param_.include_snapshot = true;
		param_.depth = 2;
		param_.max_keys = 1000;
		set_cpool_events_attrs(&cev_attrs, 0, 0,
							   io_helper()->get_account().account_id, "");

		try
		{
			while (true)
			{
				param_.truncated = false;
				io_helper()->list_object(account().container_cmo,
										 param_);
				ilog(IL_DEBUG, "\n\nProcessed objects: %ld\n", count_);
				if (!param_.truncated)
				{
					break; // no more results.
				}
				// otherwise resume from the next key
				param_.cookies_in = param_.cookies_out;
				param_.cookies_out.clear();
			}
		}
		CATCH_CLAPI_EXCEPTION(&cev_attrs, error, true)

	out:
		if (ckpt_data)
		{
			free((void *)ckpt_data);
			ckpt_data = NULL;
		}
		isi_error_handle(error, error_out);
	}

} /* end of the anonymous namespace */

void isi_cbm_restore_coi_for_acct(const char *cluster_id, uint32_t acct_id,
								  struct timeval dod, void *ckpt_ctx, get_ckpt_data_func_t get_ckpt_cb,
								  set_ckpt_data_func_t set_ckpt_cb, struct isi_error **error_out)
{
	isi_error *error = NULL;
	cbm_coi_restorer restorer(cluster_id, acct_id, dod, ckpt_ctx,
							  get_ckpt_cb, set_ckpt_cb);

	restorer.restore_objects(&error);
	ON_ISI_ERROR_GOTO(out, error, "failed to restore_objects (%s %d)",
					  cluster_id, acct_id);
out:
	isi_error_handle(error, error_out);
}
