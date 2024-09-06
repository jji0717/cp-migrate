#include <errno.h>
#include <pthread.h>
#include <iostream>

#include <ifs/ifs_lin_open.h>
#include <isi_util/isi_error.h>
#include <isi_util_cpp/scoped_lock.h>
#include <isi_cpool_d_common/isi_cloud_object_id.h>
#include <isi_ufp/isi_ufp.h>

#include "io_helper/isi_cbm_ioh_base.h"
#include "io_helper/isi_cbm_ioh_creator.h"
#include "isi_cbm_account_util.h"
#include "isi_cbm_coi.h"
#include "isi_cbm_data_stream.h"
#include "isi_cbm_file.h"
#include "isi_cbm_error.h"
#include "isi_cbm_error_util.h"
#include "isi_cbm_policyinfo.h"
#include "isi_cbm_read.h"
#include "isi_cbm_write.h"
#include "isi_cbm_error.h"
#include "isi_cbm_error_util.h"
#include "isi_cbm_scoped_ppi.h"
#include "isi_cbm_archive_ckpt.h"
#include "isi_cbm_util.h"

#include "isi_cbm_gc.h"

void isi_cbm_local_gc(ifs_lin_t lin, ifs_snapid_t snapid,
					  isi_cloud_object_id object_id, const struct timeval &deleted_stub_dod,
					  struct isi_cfm_policy_provider *ppi, void *tch,
					  get_ckpt_data_func_t get_ckpt_data_func,
					  set_ckpt_data_func_t set_ckpt_data_func, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	isi_cbm_coi_sptr coi;
	std::auto_ptr<coi_entry> coi_entry;

	coi = isi_cbm_get_coi(&error);
	ON_ISI_ERROR_GOTO(out, error);

	coi_entry.reset(coi->get_entry(object_id, &error));
	ON_ISI_ERROR_GOTO(out, error);

	/*
	 * Remove the reference (as appropriate) from the COI, even if this
	 * cluster is not in the permit list.  (The permission check is done as
	 * part of cloud GC.)
	 */
	ilog(IL_DEBUG,
		 "removing reference to object ID %{} by LIN/snapid %{} "
		 "(deleted_stub_dod: %lu)",
		 isi_cloud_object_id_fmt(&object_id),
		 lin_snapid_fmt(lin, snapid), deleted_stub_dod.tv_sec);

	coi->remove_ref(object_id, lin, snapid, deleted_stub_dod, NULL,
					&error);
	ON_ISI_ERROR_GOTO(out, error,
					  "failed to remove reference to object ID %{} "
					  "by LIN/snapID %{}",
					  isi_cloud_object_id_fmt(&object_id),
					  lin_snapid_fmt(lin, snapid));

out:
	isi_error_handle(error, error_out);
}

void update_io_helper(const isi_cfm_mapentry &entry,
					  isi_cbm_ioh_base_sptr &current_ioh, struct isi_error **error_out)
{
	isi_cbm_id entry_account_id = entry.get_account_id();
	struct isi_error *error = NULL;

	/*
	 * If the account stored in current_ioh matches the one stored in
	 * entry, then return current_ioh.  Otherwise, delete it and return the
	 * new IO helper.
	 */
	if (current_ioh == NULL ||
		!entry_account_id.equals(current_ioh->get_account().account_id))
	{
		isi_cbm_account account;
		current_ioh =
			isi_cpool_get_io_helper(entry_account_id, &error);///////初始化一个ran_io_helper,根据account_id,可以提供cdo_container,cmo_container
		ON_ISI_ERROR_GOTO(out, error);
	}

out:
	isi_error_handle(error, error_out);
}

static void
get_cmo_by_ioh(coi_entry &coi_entry, isi_cbm_ioh_base_sptr cmo_io_helper,
			   cl_cmo_ostream &ostrm, ifs_lin_t lin,
			   bool send_celog_event, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	/// cdo: /d0007433042f00be23464b510af308c44dc04i4/0012/0150/00703403240f560c17460501fd3aea26_00000001_0
	///cmo:  00703403240f560c17460501fd3aea26_00000000_0
	//object_id: 00703403240f560c17460501fd3aea26
	struct cl_object_name cloud_object_name = {
		.container_cmo = coi_entry.get_cmo_container(),
		.container_cdo = "unused",
		.obj_base_name = coi_entry.get_object_id()};
	////////读metadata blob  isi_cbm_read.h,读到后解析的数据保存在ostrm中
	isi_cph_read_md_blob(ostrm, lin, cloud_object_name,
						 false, false,
						 cmo_io_helper.get(), send_celog_event, &error);
}

/**
 * get the cmo stream and return the io helper object to reuse it
 */
static isi_cbm_ioh_base_sptr
get_cmo_by_acct(coi_entry &coi_entry, const isi_cbm_account &acct,
				cl_cmo_ostream &ostrm, ifs_lin_t lin, bool send_celog_event,
				struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	isi_cbm_ioh_base_sptr io_helper(                          //////有个bug关于这个io_helper:最早没有用shared_ptr.搜这个文件的历史
		isi_cbm_ioh_creator::get_io_helper(acct.type/*RAN*/, acct)); ////全局变量:最开始就创建好了g_ran_io_helper

	if (!io_helper)
	{
		const isi_cbm_id &account_id =
			coi_entry.get_archive_account_id();

		error = isi_cbm_error_new(CBM_NO_IO_HELPER,
								  "failed to get I/O helper for account "
								  "(ID: %u cluster: %s)",
								  account_id.get_id(),
								  account_id.get_cluster_id());
		goto out;
	}

	/////////返回的cmo数据保存在ostrm中
	get_cmo_by_ioh(coi_entry, io_helper, ostrm, lin, send_celog_event,
				   &error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);

	return io_helper;
}

static void
get_account_of_coi(struct isi_cfm_policy_provider *ppi, coi_entry &coi_entry,
				   isi_cbm_account &acct, struct isi_error **error_out)
{
	const isi_cbm_id &account_id = coi_entry.get_archive_account_id();

	isi_cfm_get_account(
		(isi_cfm_policy_provider *)ppi, account_id, acct, error_out);

	if (*error_out)
	{
		isi_error_add_context(*error_out,
							  "failed to get account information (ID: %u cluster: %s)",
							  account_id.get_id(), account_id.get_cluster_id());
	}
}

template <typename T>
static std::auto_ptr<isi_cloud::coi_entry>
get_neighbor_existing_cmo(isi_cbm_coi_sptr coi, isi_cloud_object_id oid,
						  cl_cmo_ostream &ostrm, ifs_lin_t lin, struct isi_cfm_policy_provider *ppi,
						  T neighbor_func, struct isi_error **error_out)
{
	struct isi_cbm_account acct;
	struct isi_error *error = NULL;
	std::auto_ptr<isi_cloud::coi_entry> coi_entry;
	bool send_celog_event = false;

	do
	{
		if (error)
		{
			isi_error_free(error);
			error = NULL;
		}
		coi_entry.reset(NULL);

		if (neighbor_func(coi, oid, &error))
		{
			coi_entry.reset(coi->get_entry(oid, &error));
			ON_ISI_ERROR_GOTO(out, error);
		}
		else
			break;

		get_account_of_coi(ppi, *coi_entry, acct, &error);
		ON_ISI_ERROR_GOTO(out, error);

		get_cmo_by_acct(*coi_entry, acct, ostrm, lin,
						send_celog_event, &error);
	} while (isi_cbm_error_is_a(error, CBM_CLAPI_OBJECT_NOT_FOUND));

out:
	isi_error_handle(error, error_out);
	return coi_entry;
}
///为了cloud_gc的测试接口:isi_cbm_cloud_gc(oid, NULL, NULL, NULL, NULL, &error);
void isi_cbm_cloud_gc_ex(const isi_cloud_object_id &object_id,
						struct isi_cfm_policy_provider *passed_in_ppi, void *tch,
					  	get_ckpt_data_func_t get_ckpt_data_func,
					  	set_ckpt_data_func_t set_ckpt_data_func, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	std::auto_ptr<isi_cloud::coi_entry> coi_entry;
	isi_cbm_coi_sptr coi;
	isi_cbm_ioh_base_sptr cmo_io_helper;
	ifs_lin_t lin = 0;

	coi = isi_cbm_get_coi(&error);
	coi_entry.reset(coi->get_entry(object_id, &error));/////根据object_id,coi从kernel中获取coi_entry. 假装这个coi_entry是ooi_entry??????
	cl_cmo_ostream cmo_strm; ///cl_provider_common.cpp中的curl_write_callback_resp: 用来接收从libcurl下载的数据流,并负责解析
	cmo_strm.set_object_id(object_id);
	struct isi_cbm_account cmo_account;
	////container_cdo, container_cmo, obj_base_name
	struct cl_object_name  cloud_object_name;   
	
	bool cmo_found = true;
	printf("\n%s called\n", __func__);
	struct isi_cfm_policy_provider *ppi;
	scoped_ppi_reader ppi_reader;
	ppi = (struct isi_cfm_policy_provider*)ppi_reader.get_ppi(&error);
	get_account_of_coi(ppi, *coi_entry, cmo_account, &error);
	ON_ISI_ERROR_GOTO(out, error, "failed to get ppi");
	printf("%s called get account of coi successfully\n", __func__);
	/////拿cmo(metadata): 调用isi_cph_read_md_blob(cmo_strm,lin,cloud_object_name)
	cmo_io_helper = get_cmo_by_acct(*coi_entry, cmo_account, cmo_strm, lin, false,&error);

	if (isi_cbm_error_is_a(error, CBM_CLAPI_OBJECT_NOT_FOUND))
	{
		ilog(IL_DEBUG, "skip error: %#{}", isi_error_fmt(error));
		isi_error_free(error);
		error= NULL;
		cmo_found = false;
	}
	ON_ISI_ERROR_GOTO(out, error);
	printf("%s called cmo_found successfully\n", __func__);
	if (cmo_found)
	{
		printf("%s called cmo_found\n", __func__);
		cloud_object_name.container_cmo = coi_entry->get_cmo_container();
		cloud_object_name.container_cdo = "unused";
		cloud_object_name.obj_base_name = coi_entry->get_object_id();
		isi_cbm_remove_cloud_objects(cmo_strm.get_mapinfo(),
									NULL,
									NULL,
									cloud_object_name,
									cmo_io_helper.get(),
									false,
									NULL,
									NULL,
									NULL,
									&error);
		if (error != NULL)
		{
			printf("%s called isi_cbm_remove_cloud_objects failed\n", __func__);
		}
	}
out:
	if (error)
	{
		printf("%s called finish cloud gc with error\n", __func__);
	}
	else{
		printf("%s called finish cloud gc successfully\n", __func__);
	}
}
void isi_cbm_cloud_gc(const isi_cloud_object_id &object_id,
					  struct isi_cfm_policy_provider *passed_in_ppi, void *tch,
					  get_ckpt_data_func_t get_ckpt_data_func,
					  set_ckpt_data_func_t set_ckpt_data_func, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	isi_cbm_coi_sptr coi;
	std::auto_ptr<isi_cloud::coi_entry> coi_entry;
	char archiving_cluster_id_buf[IFSCONFIG_GUID_STR_SIZE + 1];
	std::auto_ptr<isi_cloud::coi_entry> coi_entry_prev;
	std::auto_ptr<isi_cloud::coi_entry> coi_entry_next;
	struct isi_cbm_account cmo_account;
	struct isi_cbm_account cmo_account_prev;
	struct isi_cbm_account cmo_account_next;
	isi_cbm_ioh_base_sptr cmo_io_helper;
	struct cl_object_name cloud_object_name;
	cl_cmo_ostream cmo_strm, cmo_strm_prev, cmo_strm_next;
	btree_key_t ooi_entry_key;
	bool send_celog_event = false;

	ifs_lin_t lin = 0;
	bool coid_locked = false;
	bool cmo_found = true;

	ilog(IL_DEBUG, "Cloud GCing (%#{})",
		 isi_cloud_object_id_fmt(&object_id));
	printf("\n%s called\n", __func__);
	/*
	 * Initialize the COI; from the COI we can retrieve information needed
	 * to read the CMO (specifically, the account used to archive the CMO
	 * and the container to which the CMO was written).
	 */
	coi = isi_cbm_get_coi(&error);
	ON_ISI_ERROR_GOTO(out, error);

	coi->lock_coid(object_id, false, &error);
	ON_ISI_ERROR_GOTO(out, error);
	coid_locked = true;

	coi_entry.reset(coi->get_entry(object_id, &error));
	ON_ISI_ERROR_GOTO(out, error,
					  "failed to get COI entry for object ID (%#{})",
					  isi_cloud_object_id_fmt(&object_id));

	/*
	 * Return if the coi entry refers to an absent ooi entry.
	 */
	ooi_entry_key = coi_entry->get_ooi_entry_key();
	if ((ooi_entry_key.keys[0] == coi_entry::NULL_BTREE_KEY.keys[0]) &&
		(ooi_entry_key.keys[1] == coi_entry::NULL_BTREE_KEY.keys[1]))
	{
		ilog(IL_DEBUG, "No ooi entry for coi entry for "
					   "object ID (%s, %ld) so no cloud gc to be done",
			 object_id.to_c_string(), object_id.get_snapid());
		printf("%s called no ooid entry for coi entry\n", __func__);
		//goto out;  //for test jjz
	}

	/*
	 * Retrieve the GUID of the cluster which archived this file - we'll
	 * use it for the permission check later on.
	 */
	getGuid(coi_entry->get_archiving_cluster_id(),
			archiving_cluster_id_buf, IFSCONFIG_GUID_STR_SIZE);
	archiving_cluster_id_buf[IFSCONFIG_GUID_STR_SIZE] = '\0';

	/*
	 * Use the account information from the COI entry to create an IO
	 * helper for reading the CMO; from the CMO we can retrieve the stub
	 * map.
	 */
	// Access config under read lock

	{
		scoped_ppi_reader ppi_reader;
		struct isi_cfm_policy_provider *ppi = passed_in_ppi;
		if (ppi == NULL)
		{
			ppi = (struct isi_cfm_policy_provider *)
					  ppi_reader.get_ppi(&error);
			ON_ISI_ERROR_GOTO(out, error, "failed to get ppi");
		}

		/*
		 * Permission check - make sure this cluster is allowed to
		 * delete this object from the cloud.
		 * (true means it is ok to GC when the guid is being removed
		 * from the permit list)
		 */
		cluster_permission_check(archiving_cluster_id_buf, ppi,
								 true, &error);
		ON_ISI_ERROR_GOTO(out, error);

		auto prev_func = [](isi_cbm_coi_sptr coi, isi_cloud_object_id &oid,
							struct isi_error **error_out) -> bool
		{
			return coi->get_prev_version(oid, oid, error_out);
		};
		auto next_func = [](isi_cbm_coi_sptr coi, isi_cloud_object_id &oid,
							struct isi_error **error_out) -> bool
		{
			return coi->get_next_version(oid, oid, error_out);
		};

		coi_entry_prev = get_neighbor_existing_cmo(coi, object_id,
												   cmo_strm_prev, lin, ppi, prev_func, &error);
		ON_ISI_ERROR_GOTO(out, error);

		coi_entry_next = get_neighbor_existing_cmo(coi, object_id,
												   cmo_strm_next, lin, ppi, next_func, &error);
		ON_ISI_ERROR_GOTO(out, error);

		get_account_of_coi(ppi, *coi_entry, cmo_account, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	// set the correct object_id, CMO on Azure has obsolete object_id
	printf("%s called cmo_strm.set_object_id", __func__);
	cmo_strm.set_object_id(object_id);

	cmo_io_helper = get_cmo_by_acct(
		*coi_entry, cmo_account, cmo_strm, lin/*不用考虑*/, send_celog_event, /////获得的cmo放进cmo_strm
		&error);
	// suppose the CMO is gone, skip the error
	if (isi_cbm_error_is_a(error, CBM_CLAPI_OBJECT_NOT_FOUND))
	{
		ilog(IL_DEBUG, "skip error: %#{}", isi_error_fmt(error));

		isi_error_free(error);
		error = NULL;
		cmo_found = false;
	}
	ON_ISI_ERROR_GOTO(out, error);

	if (cmo_found)
	{
		// This is the point at which we will attempt to delete
		// cloud objects.  Mark it as the start of GC in the coi entry.
		coi->removing_object(object_id, &error);  ///jjz 
		ON_ISI_ERROR_GOTO(out, error,
						  "failed to set cloud object removal for object ID(%s, %ld)",
						  object_id.to_c_string(), object_id.get_snapid());

		cloud_object_name.container_cmo = coi_entry->get_cmo_container();
		cloud_object_name.container_cdo = "unused";
		cloud_object_name.obj_base_name = coi_entry->get_object_id();
		isi_cbm_remove_cloud_objects(cmo_strm.get_mapinfo(),
									 coi_entry_prev.get() ? &cmo_strm_prev.get_mapinfo() : NULL,
									 coi_entry_next.get() ? &cmo_strm_next.get_mapinfo() : NULL,
									 cloud_object_name,
									 cmo_io_helper.get(),
									 false, // not sync_rollback
									 get_ckpt_data_func,
									 set_ckpt_data_func,
									 tch,
									 &error);
		if (error != NULL)
		{
			isi_error_add_context(error, "failed to remove cloud "
										 "objects for (cloud object id: %s %ld)",
								  object_id.to_c_string(), object_id.get_snapid());
			goto out;
		}
	}

	// fail point to avoid removing the coi entry
	{
		bool fail_cloud_gc_remove_coi_hit = false;
		UFAIL_POINT_CODE(fail_cloud_gc_remove_coi, {
			fail_cloud_gc_remove_coi_hit = true;
		});
		if (fail_cloud_gc_remove_coi_hit)
			goto out;
	}

	/*
	 * With all cloud objects (CDOs and CMO) deleted, remove the entry from
	 * the COI.
	 */
	coi->remove_object(object_id, false, &error);
	if (error)
	{
		isi_error_add_context(error,
							  "failed to remove object (%s) from COI",
							  object_id.to_c_string());
		goto out;
	}

	// cleanup the mapinfo, delete the store if any
	if (cmo_found && cmo_strm.get_mapinfo().is_overflow())
	{
		isi_cfm_mapinfo &mapinfo =
			const_cast<isi_cfm_mapinfo &>(cmo_strm.get_mapinfo());

		mapinfo.remove_store(&error);
		ON_ISI_ERROR_GOTO(out, error,
						  "Failed to remove the mapentry store");
	}

out:
	if (error)
	{
		ilog(IL_DEBUG, "Finished Cloud GC (%s, %ld), error: %#{}",
			 object_id.to_c_string(), object_id.get_snapid(),
			 isi_error_fmt(error));
		isi_error_handle(error, error_out);
	}
	else
	{
		ilog(IL_DEBUG, "Finished Cloud GC (%s, %ld) successfully",
			 object_id.to_c_string(), object_id.get_snapid());
	}

	if (coid_locked)
	{
		coi->unlock_coid(object_id, isi_error_suppress(IL_ERR));
		coid_locked = false;
	}
}

void remove_cdo(struct cl_object_name &cloud_object_name,
				isi_cbm_ioh_base_sptr cdo_io_helper, ifs_lin_t lin, int index, off_t offset,
				struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	cpool_events_attrs cev_attrs = {0};

	try
	{
		std::string entityname = get_entityname(
			cloud_object_name.container_cdo,
			cdo_io_helper->get_cdo_name(
				cloud_object_name.obj_base_name, index));

		set_cpool_events_attrs(&cev_attrs, lin, offset,
							   cdo_io_helper->get_account().account_id, entityname);
		/////删除云端的cdo文件.  eg: /ifs/ran/d0007433042f00be23464b510af308c44dc04i4/0050/0058
		cdo_io_helper->delete_cdo(cloud_object_name, index,
								  cloud_object_name.obj_base_name.get_snapid());

		// This call will trigger fail point and
		// generate error for the generation of
		// cloudpool_event.
		trigger_failpoint_cloudpool_events();
	}
	CATCH_CLAPI_EXCEPTION(&cev_attrs, error, false)

	if (isi_cbm_error_is_a(error, CBM_CLAPI_OBJECT_NOT_FOUND))
	{
		ilog(IL_DEBUG, "Couldn't find cloud object"
					   "cdo %s snap %lld error %#{}\n",
			 cloud_object_name.container_cdo.c_str(),
			 cloud_object_name.obj_base_name.get_snapid(),
			 isi_error_fmt(error));
		isi_error_free(error);
		error = NULL;
	}
	else
	{
		send_cloudpools_event(&cev_attrs, error, __FUNCTION__);
	}

	isi_error_handle(error, error_out);
}

void isi_cbm_remove_cloud_objects(const isi_cfm_mapinfo &stub_map,
								  const isi_cfm_mapinfo *stub_map_prev,
								  const isi_cfm_mapinfo *stub_map_next,
								  struct cl_object_name &cloud_object_name,
								  isi_cbm_ioh_base *cmo_io_helper,
								  bool sync_rollback,
								  get_ckpt_data_func_t get_ckpt_data_func,
								  set_ckpt_data_func_t set_ckpt_data_func,
								  void *opaque,
								  struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	off_t offset = 0;
	isi_cbm_ioh_base_sptr cdo_io_helper;
	void *ckpt_data = NULL;
	size_t ckpt_data_size = -1;
	djob_op_job_task_state task_state;
	isi_cfm_mapinfo::map_iterator iter;
	uint64_t cloud_snapid;
	struct cl_object_name cmo_cloud_object_name = cloud_object_name;
	u_int64_t ckpt_filerev = 0;
	u_int32_t ckpt_state = CKPT_STATE_NOT_SET;
	std::string entityname;
	ifs_lin_t lin = 0;
	cpool_events_attrs cev_attrs = {0};

	/*
	 * Get the checkpoint information if there is any.  We need to have
	 * this when we check the stub state since the file could have been
	 * stubbed but the invalidate could have failed.
	 */
	// if (get_ckpt_data_func != NULL)
	// {
	// 	get_ckpt_data_func(opaque, &ckpt_data, &ckpt_data_size,
	// 					   &task_state, 0);
	// 	printf("%s called try to get_ckpt_data_func\n", __func__);
	// 	if (ckpt_data_size > 0 && ckpt_data != NULL)
	// 	{
	// 		isi_cbm_archive_ckpt ckpt;
	// 		ckpt.unpack(ckpt_data, ckpt_data_size, &error);
	// 		if (error)
	// 		{
	// 			isi_error_add_context(error,
	// 								  "Failed to unpack mapinfo for cloud object"
	// 								  "cdo %s snap %lld",
	// 								  cloud_object_name.container_cdo.c_str(),
	// 								  cloud_object_name.obj_base_name.get_snapid());
	// 			free((void *)ckpt_data);
	// 			ckpt_data = NULL;
	// 			goto out;
	// 		}
	// 		ckpt_filerev = ckpt.get_filerev();
	// 		ckpt_state = ckpt.get_ckpt_state();
	// 		offset = ckpt.get_offset();
	// 		free((void *)ckpt_data);
	// 		printf("%s called ckpt_state:%d offset:%ld\n", __func__, ckpt_state, offset);
	// 	}
	// }

	/*
	 * With the stub map, we can start to delete CDOs.
	 *
	 * Iterate over each entry in the stub map.We'll need to
	 * individually delete each CDO that makes up the data
	 * represented by the map entry.
	 */
	/////通过迭代stub_map中的每一个map,可以删除map对应的每一个CDO
	printf("%s called before gc each chunk in loop, offset:%ld\n", __func__, offset);
	stub_map.get_containing_map_iterator_for_offset(iter, offset, &error);

	ON_ISI_ERROR_GOTO(out, error);
	////因为在archive的时候,上传的文件大小超过一个chunksize的大小,需要分几个chunk上传,
	///因此上传时,mapinfo中放了多个pmapentry
	for (; iter != stub_map.end(); iter.next(&error))
	{
		const isi_cfm_mapentry &map_entry = iter->second;

		ilog(IL_DEBUG, "Checking object %#{} idx: %d, off: %lu, len: %lu",
			 isi_cloud_object_id_fmt(&map_entry.get_object_id()),
			 map_entry.get_index(), map_entry.get_offset(),
			 map_entry.get_length());
		// rollback operations removes cloud object that has same
		// object_id to the wip_mapinfo
		if (sync_rollback &&
			stub_map.get_object_id() != map_entry.get_object_id())
			continue;

		/*
		 * We may need a different IO helper object based on the
		 * account ID for this map entry.  Also, update the cloud
		 * object name using the information stored in this map entry.
		 */
		//////根据map_entry中的account_id,初始化ran_io_helper，可以获得cdo_container,cmo_container信息
		update_io_helper(map_entry, cdo_io_helper, &error); /////拿RAN的io_helper
		ON_ISI_ERROR_GOTO(out, error, "offset: %zu length: %zu",
						  map_entry.get_offset(), map_entry.get_length());

		cloud_object_name.container_cmo = "unused";
		cloud_object_name.container_cdo = map_entry.get_container();
		cloud_object_name.obj_base_name = map_entry.get_object_id();
		cloud_snapid = cloud_object_name.obj_base_name.get_snapid();

		size_t bytes_to_delete = map_entry.get_length();
		size_t chunksize = stub_map.get_chunksize();
		/* CMO index is 0, CDOs are indexed from 1. */
		/////mapinfo.set_chunksize(blobinfo.chunksize);  isi_cbm_archive.cpp: isi_cbm_archive_common中设置
		int index = 1 + map_entry.get_offset() / chunksize;
		// If we are resuming from a checkpoint
		// calculate bytes_to_delete and the index
		// that are appropriate for the offset (basically
		// continue the opeartion where we stopped).
		// Otherwise offset is updated so we will
		// save the right value to the checkpoint.
		// This will account for in continous mapping.
		////map_entry.get_offset()始终为0.
		///当需要从checkpoint的数据继续cloud gc时,需要根据文件总长度(map_entry.get_length()),当前offset(从checkpoint中读取),
		///从而计算出bytes_to_delete, index. index = offset / chunksize
		if (offset > map_entry.get_offset() &&
			(size_t)offset < map_entry.get_offset() +
								 map_entry.get_length())
		{
			bytes_to_delete =
				map_entry.get_length() +
				map_entry.get_offset() - offset;
			index += (offset - map_entry.get_offset()) /
					 chunksize;
			printf("%s called map_length:%ld map_offset:%ld offset:%ld index:%d\n", __func__, map_entry.get_length(),
				  map_entry.get_offset(), offset, index);
		}
		else
			offset = map_entry.get_offset();
		/////////如果在上一次的cloud gc中的set_ckpt_data_func中保存了那个时刻的offset,
		////////那么这一次的cloud gc中,bytes_to_delete = map_entry.get_length()-offset (offset就是get_ckpt_data_func中读出的offset,值与前一个时刻保存的值相同)
		printf("%s called index:%d bytes_to_delete:%ld offset:%ld\n", __func__, index, bytes_to_delete,offset);
		while (bytes_to_delete > 0)
		{
			bool is_shared = false;
			bool shared_by_pre = false;

			// check the previous version when the CDO
			// is not created by the current version
			if (map_entry.get_object_id() !=
					stub_map.get_object_id() &&
				stub_map_prev)
			{
				is_shared = stub_map_prev->contains(
					offset, map_entry.get_object_id(),
					&error);
				ON_ISI_ERROR_GOTO(out, error);

				if (is_shared)
					shared_by_pre = true;
			}

			// check with the next version
			if (!is_shared && stub_map_next)
			{
				is_shared = stub_map_next->contains(
					offset, map_entry.get_object_id(),
					&error);
				ON_ISI_ERROR_GOTO(out, error);
			}

			if (is_shared)
			{
				const isi_cfm_mapinfo *nb_map =
					shared_by_pre ? stub_map_prev : stub_map_next;
				ilog(IL_DEBUG, "%#{} is used by the "
							   "%s version %#{}. offset: %lu "
							   "index: %d",
					 isi_cloud_object_id_fmt(
						 &map_entry.get_object_id()),
					 shared_by_pre ? "previous" : "next",
					 isi_cloud_object_id_fmt(
						 &nb_map->get_object_id()),
					 offset, index);
			}

			// not shared with neighbor versions, collect it
			if (!is_shared)
			{
				isi_cloud_object_id &oid =
					cloud_object_name.obj_base_name; /////object_id

				ilog(IL_INFO, "Removing cloud object: "
							   "%#{}, idx :%d",
					 isi_cloud_object_id_fmt(&oid), index);
				///如果这个大文件被分成几个chunk,那么每个chunk的index不同,remove_cdo每次删除一块chunk(删除cdo container目录下的一个目录)
				/////多个chunk的文件,在cdo container目录下会有多个不同目录包含这块chunk
				remove_cdo(cloud_object_name,
						   cdo_io_helper, lin, index, offset,
						   &error);
				printf("%s called remove_cdo index:%d offset:%ld\n", __func__, index, offset);
				ON_ISI_ERROR_GOTO(out, error,
								  "failed to delete CDO "
								  "(cloud object id: %s index: %d "
								  "bytes left to delete: %zu)",
								  oid.to_c_string(), index,
								  bytes_to_delete);
			}

			if (bytes_to_delete < chunksize)
				bytes_to_delete = 0;
			else
				bytes_to_delete -= chunksize;
			offset += chunksize;
			++index;


			/////////针对有set_ckpt_data_func的情况下，对于每一个需要删除的chunk，都会有其对应的checkpoint
			//////如果此刻发现task_state变成了PAUSED，那么退出cloud gc
			// if (set_ckpt_data_func != NULL)
			// {
			// 	/////删除cdo的时候记录offset,如果task被pause了,offset保存在ckpt中,这样下次cloud gc开始时读取ckpt,直接从offset开始继续删除
			// 	isi_cbm_archive_ckpt ckpt(ckpt_filerev,
			// 							  offset, stub_map, &error);

			// 	ON_ISI_ERROR_GOTO(out, error,
			// 					  "Failed to construct archive checkpoint object");

			// 	ckpt.set_ckpt_state(ckpt_state);
			// 	ckpt.pack(&error);
			// 	ON_ISI_ERROR_GOTO(out, error,
			// 					  "Failed to pack the checkpoint");

			// 	set_ckpt_data_func(opaque, ckpt.get_serialized_ckpt(),
			// 					   ckpt.get_packed_size(),
			// 					   &task_state, 0);

			// 	if (task_state == DJOB_OJT_PAUSED)
			// 	{
			// 		error = isi_cbm_error_new(CBM_FILE_PAUSED,
			// 								  "GC Paused for cloud object id %s",
			// 								  cloud_object_name.obj_base_name.to_c_string());
			// 		printf("%s called set_ckpt_data_func!= NULL && task_state is PAUSED, index=%d offset:%ld\n", __func__, index, offset);
			// 		goto out;
			// 	}

			// 	if (task_state == DJOB_OJT_STOPPED)
			// 	{
			// 		error =
			// 			isi_cbm_error_new(CBM_FILE_STOPPED,
			// 							  "GC Stopped "
			// 							  "for cloud object id %s",
			// 							  cloud_object_name.obj_base_name.to_c_string());
			// 		goto out;
			// 	}
			// }
			UFAIL_POINT_CODE(fail_cloud_gc, {if (index > 5) ASSERT(0); });
		}
	}

	ON_ISI_ERROR_GOTO(out, error, "Failed to move forward the iterator");

	/*
	 * With all the CDOs deleted, we can delete the CMO.
	 */
	try
	{
		entityname = get_entityname(cloud_object_name.container_cdo,
									cmo_io_helper->get_cmo_name(cloud_object_name.obj_base_name));

		set_cpool_events_attrs(&cev_attrs, lin, -1,
							   cmo_io_helper->get_account().account_id, entityname);
		//删除cmo目录下的metadata文件. eg: /ifs/ran/m0007433042f00be23464b510af308c44dc04i4/0094/0188 
		cmo_io_helper->delete_cmo(cmo_cloud_object_name,
								  cmo_cloud_object_name.obj_base_name.get_snapid());

		// This call will trigger fail point and
		// generate error for the generation of
		// cloudpool_event.
		trigger_failpoint_cloudpool_events();
	}
	CATCH_CLAPI_EXCEPTION(&cev_attrs, error, false)

	if (isi_cbm_error_is_a(error, CBM_CLAPI_OBJECT_NOT_FOUND))
	{
		ilog(IL_DEBUG, "Couldn't find cloud object"
					   "cmo %s snap %lld error %#{}\n",
			 cloud_object_name.container_cmo.c_str(),
			 cloud_object_name.obj_base_name.get_snapid(),
			 isi_error_fmt(error));
		isi_error_free(error);
		error = NULL;
	}
	else
	{
		send_cloudpools_event(&cev_attrs, error, __FUNCTION__);
	}

	if (error != NULL)
	{
		isi_error_add_context(error,
							  "failed to delete CMO (cloud object id: %s)",
							  cloud_object_name.obj_base_name.to_c_string());
		goto out;
	}
out:
	isi_error_handle(error, error_out);
}

/**
 * dump the version chain of the cloud object
 */
void isi_cbm_dump_version_chain(const isi_cloud_object_id &object_id)
{
	struct isi_error *error = NULL;

	isi_cloud::coi coi;
	isi_cloud_object_id cobj_id, pobj_id;

	coi.initialize(&error);
	ON_ISI_ERROR_GOTO(out, error);

	std::cout << "versions: ";

	if (!coi.get_last_version(object_id, cobj_id, &error) || error)
		goto out;

	std::cout << cobj_id.get_snapid() << " ";
	pobj_id = cobj_id;

	while (coi.get_prev_version(pobj_id, cobj_id, &error) && !error)
	{
		std::cout << cobj_id.get_snapid() << " ";
		pobj_id = cobj_id;
	}
out:
	std::cout << std::endl;

	if (error)
	{
		ilog(IL_ERR, "failed to dump version list: %#{}",
			 isi_error_fmt(error));
		isi_error_free(error);
	}
}

/**
 * dump the CDOs of the version
 */
void isi_cbm_dump_cdo(const isi_cloud_object_id &object_id)
{
	struct isi_error *error = NULL;
	cl_cmo_ostream ostrm;
	isi_cbm_coi_sptr coi;
	isi_cbm_account acct;
	std::auto_ptr<isi_cloud::coi_entry> coi_entry;
	bool send_celog_event = true;

	coi = isi_cbm_get_coi(&error);
	ON_ISI_ERROR_GOTO(out, error);

	coi_entry.reset(coi->get_entry(object_id, &error));
	ON_ISI_ERROR_GOTO(out, error,
					  "failed to get COI entry for object: %s",
					  object_id.to_c_string());

	{
		scoped_ppi_reader ppi_reader;

		struct isi_cfm_policy_provider *ppi;

		ppi = (struct isi_cfm_policy_provider *)
				  ppi_reader.get_ppi(&error);
		ON_ISI_ERROR_GOTO(out, error, "failed to get ppi");

		get_account_of_coi(ppi, *coi_entry, acct, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	get_cmo_by_acct(*coi_entry, acct, ostrm, 0 /*lin for celog*/,
					send_celog_event, &error);
	ON_ISI_ERROR_GOTO(out, error);

	const_cast<isi_cfm_mapinfo &>(ostrm.get_mapinfo()).dump(true, true);
out:
	if (error)
	{
		ilog(IL_ERR, "failed to dump CDO: %#{}", isi_error_fmt(error));
		isi_error_free(error);
	}
}
