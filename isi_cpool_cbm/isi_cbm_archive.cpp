#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h> // XXXegc: debug
#include <ifs/ifs_types.h>
#include <ifs/ifs_lin_open.h>
#include <ifs/ifs_syscalls.h>
#include <ifs/bam/bam_pctl.h>
#include <isi_util/syscalls.h>
#include <isi_ufp/isi_ufp.h>

#include <isi_cloud_common/isi_cpool_version.h>
#include "isi_cpool_cbm/io_helper/isi_cbm_ioh_creator.h"
#include "isi_cpool_cbm/io_helper/isi_cbm_ioh_base.h"
#include "isi_cpool_cbm/isi_cpool_cbm.h"
#include "isi_cpool_cbm/isi_cbm_account_util.h"
#include "isi_cpool_cbm/isi_cbm_policyinfo.h"
#include "isi_cpool_cbm/isi_cbm_mapper.h"
#include "isi_cpool_cbm/isi_cbm_snapshot.h"
#include "isi_cpool_cbm/isi_cbm_scoped_coid_lock.h"
#include "isi_cpool_cbm/isi_cbm_scoped_flock.h"
#include "isi_cpool_cbm/isi_cbm_write.h"
#include "isi_cpool_cbm/isi_cbm_util.h"
#include "isi_cpool_cbm/isi_cbm_gc.h"
#include "isi_cpool_cbm/isi_cbm_archive_ckpt.h"
#include "isi_cpool_cbm/isi_cbm_invalidate.h"
#include "isi_cpool_cbm/isi_cbm_file.h"

#include <isi_cpool_d_common/cpool_d_common.h>
#include "isi_cbm_coi.h"

#include "isi_cbm_error.h"
#include "isi_cbm_error_util.h"
#include "isi_cbm_scoped_ppi.h"

struct isi_cfm_policy_provider;

static bool isi_cph_verify_stub(int, u_int64_t);

static void isi_cph_create_stub(int fd, ifs_lin_t lin, isi_cfm_mapinfo &mapinfo,
								u_int64_t filerev, bool do_async_invalidate, struct isi_error **error_out);

static void update_chunkinfo(int fd, uint64_t index, size_t fsize,
							 off_t src_offset, off_t dst_offset, size_t transfer_size,
							 bool compress, bool checksum, encrypt_ctx_p ectx,
							 const struct isi_cpool_blobinfo &blobinfo,
							 struct isi_cbm_chunkinfo &chunkinfo);

static void update_map(isi_cfm_mapinfo &mapinfo,
					   const isi_cpool_master_cdo &master_cdo,
					   const isi_cpool_blobinfo &blobinfo,
					   const struct isi_cbm_chunkinfo &chunkinfo,
					   struct isi_error **error_out);

static void write_checkpoint(ifs_lin_t lin,
							 isi_cbm_ioh_base *ioh,
							 isi_cfm_mapinfo &mapinfo,
							 uint64_t filerev,
							 get_ckpt_data_func_t get_ckpt_data_func,
							 set_ckpt_data_func_t set_ckpt_data_func,
							 void *opaque,
							 u_int32_t ckpt_state,
							 struct isi_error **error);

static void update_map_for_master_cdo(isi_cfm_mapinfo &mapinfo,
									  isi_cpool_master_cdo &master_cdo,
									  struct isi_error **error_out);

static void isi_cbm_archive_common(ifs_lin_t lin, const isi_cbm_id &,
								   const isi_cbm_id &, const isi_cbm_account &account, isi_cbm_ioh_base *ioh,
								   bool compress, bool checksum, bool encrypt, bool archive_snaps,
								   get_ckpt_data_func_t get_ckpt_data_func,
								   set_ckpt_data_func_t set_ckpt_data_func,
								   bool do_async_invalidate,
								   void *opaque,
								   struct isi_error **error_out);

static void get_object_id_from_mapinfo(const isi_cfm_mapinfo &mapinfo,
									   isi_cloud_object_id &object_id, isi_cbm_id &account_id,
									   std::string &container_cdo, struct isi_error **error_out);
static void clear_cloud_objects(isi_cfm_mapinfo &mapinfo,
								get_ckpt_data_func_t get_ckpt_data_func,
								set_ckpt_data_func_t set_ckpt_data_func,
								void *opaque,
								ifs_lin_t lin, isi_cbm_ioh_base *ioh, isi_error **error_out);
static bool file_was_deleted(ifs_lin_t lin);

/*
 * check if file extention (by lin) is a compressed one
 * like .zip, .bzip
 */
static bool is_file_compressed(
	struct isi_cfm_policy_provider *ppi, ifs_lin_t lin,
	struct isi_error **error_out);

/**
 * Given a lin, trigger the invalidate of the cache.  The invalidate code will
 * handle the case where the cache is already dirty.   This is a clean up
 * routine to handle the case where the truncate failed during the initial
 * stubbing and during the restart of the stub operation, we find the
 * CKPT_INVALIDATE_NEEDED state set in the checkpoint.
 */
void archive_ckpt_invalidate(ifs_lin_t lin, struct isi_error **error_out)
{
	bool dirty_data;

	isi_cbm_invalidate_cache(lin, HEAD_SNAPID, &dirty_data, error_out);
}

void cleanup_cloud_object(
	ifs_lin_t lin,
	get_ckpt_data_func_t get_ckpt_data_func,
	set_ckpt_data_func_t set_ckpt_data_func,
	void *opaque,
	const isi_cfm_policy_provider *cbm_cfg,
	struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	void *ckpt_data = NULL;
	size_t ckpt_data_size = -1;
	isi_cbm_ioh_base_sptr io_helper;
	struct isi_cbm_account account;
	isi_cbm_id ckpt_account_id;
	djob_op_job_task_state t_state;

	if (get_ckpt_data_func != NULL)
	{
		get_ckpt_data_func(opaque, &ckpt_data, &ckpt_data_size,
						   &t_state, 0);
		if (ckpt_data_size > 0 && ckpt_data != NULL)
		{
			isi_cbm_archive_ckpt ckpt;
			ckpt.unpack(ckpt_data, ckpt_data_size, &error);
			ON_ISI_ERROR_GOTO(out, error,
							  "Failed to unpack mapinfo for Lin %{}",
							  lin_fmt(lin));

			ckpt_account_id = ckpt.get_mapinfo().get_account();
			isi_cfm_get_account((isi_cfm_policy_provider *)cbm_cfg, ckpt_account_id,
								account, &error);
			ON_ISI_ERROR_GOTO(out, error);
			io_helper.reset(isi_cbm_ioh_creator::get_io_helper(account.type,
															   account));
			clear_cloud_objects(ckpt.get_mapinfo(),
								get_ckpt_data_func,
								set_ckpt_data_func,
								opaque,
								lin, io_helper.get(), &error);
			ON_ISI_ERROR_GOTO(out, error,
							  "Failed to delete cloud objects for %{}",
							  lin_fmt(lin));
		}
	}
out:
	free(ckpt_data);
	ckpt_data = NULL;

	isi_error_handle(error, error_out);
}

/**
 * Given a lin and the policy to be associated with that lin, archive the
 * associated file to the cloud specified via the policy specified.
 *
 * 1) Generate base container and object information given the account
 *    information.
 * 2) Obtain verification information to use after cloud and mapping write are
 *    complete to be sure that data in cloud is valid for file being archived
 *    (using filerev).
 * 3) Put the information to the cloud, generating mapping information.
 * 4) Exclusive lock the lin
 * 5) Verify confirmation information is still valid for lin (using the filerev)
 * 6) Write the acount and mapping information for the lin into the stub.
 * 7) Stash away information about the file (i.e., original mtime and size)
 * 8) Unlock the lin.
 * NB: steps 4-8 have been combined into a single system call to tighten up
 *     this operation
 */
/*
 * XXXegc: This does not handle the case where the stub already exists.  What
 *    is the policy for cleaning up old objects that are replaced and where is
 *    the old mapping information being retained for the case where the old
 *    objects are not being deleted after being replaced?
 */
/*
 * XXXegc: currently hard codes snap id to HEAD.
 */

using namespace isi_cloud;

void isi_cbm_archive_by_name(ifs_lin_t lin, const char *policy_name,
							 struct isi_cfm_policy_provider *p_policy_provider_info, void *opaque,
							 get_ckpt_data_func_t get_ckpt_data_func,
							 set_ckpt_data_func_t set_ckpt_data_func,
							 bool do_async_invalidate,
							 struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	scoped_ppi_reader ppi_rdr;
	uint32_t policy_id = 0;
	struct fp_policy *cp_policy = NULL;
	const isi_cfm_policy_provider *cbm_cfg = NULL;

	cbm_cfg = ppi_rdr.get_ppi(&error);
	ON_ISI_ERROR_GOTO(out, error);

	smartpools_get_policy_by_name(policy_name,
								  &cp_policy,
								  cbm_cfg->sp_context,
								  &error);
	// if we can not get the policy, the policy might be deleted or renamed
	if (error != NULL)
	{
		// we do not want to flush the log if the policy has been deleted
		// so log it at debug level.
		ilog(IL_DEBUG, "failed to get policy name %s: %#{}",
			 policy_name, isi_error_fmt(error));

		isi_error_free(error);
		error = NULL;

		// if the policy has been deleted and there is a checkpoint
		// clean up the cloud objects based on mapinfo
		cleanup_cloud_object(lin,
							 get_ckpt_data_func,
							 set_ckpt_data_func,
							 opaque,
							 cbm_cfg,
							 &error);
		if (error != NULL)
		{
			ilog(IL_ERR, "failed to cleanup objects for LIN %{}: %#{}",
				 lin_fmt(lin), isi_error_fmt(error));

			isi_error_free(error);
			error = NULL;
		}

		error = isi_cbm_error_new(CBM_FILE_CANCELLED,
								  "archive was cancelled because policy %s was not found",
								  policy_name);
		ON_ISI_ERROR_GOTO(out, error);
	}
	ASSERT(cp_policy != NULL);
	policy_id = cp_policy->id;
	isi_cbm_archive_by_id(lin, policy_id, p_policy_provider_info,
						  opaque,
						  get_ckpt_data_func,
						  set_ckpt_data_func,
						  do_async_invalidate,
						  &error);
	ON_ISI_ERROR_GOTO(out, error);
out:
	isi_error_handle(error, error_out);
}

void isi_cbm_archive_by_id(ifs_lin_t lin, uint32_t pol_id,
						   struct isi_cfm_policy_provider *p_policy_provider_info, void *opaque,
						   get_ckpt_data_func_t get_ckpt_data_func,
						   set_ckpt_data_func_t set_ckpt_data_func,
						   bool do_async_invalidate,
						   struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	isi_cbm_id policy_id;
	isi_cbm_id provider_id;
	bool compress = false;	   // default to no compression
	bool encrypt = false;	   // default to no encryption
	bool checksum = true;	   // default to checksum
	bool archive_snaps = true; // default to archive files with snapshot
	bool is_committed = false;
	void *ckpt_data = NULL;
	size_t ckpt_data_size = -1;
	djob_op_job_task_state t_state;
	isi_cfm_mapinfo ckpt_mapinfo(ISI_CFM_MAP_TYPE_OBJECTINFO);
	/////////typedef std::shared_ptr<class isi_cbm_ioh_base> isi_cbm_ioh_base_sptr;
	isi_cbm_ioh_base_sptr io_helper;   //////智能指针(shared_ptr),通过reset初始化

	isi_cbm_account account;

	ilog(IL_DEBUG, "Archiving %{} with policy id: %d",
		 lin_snapid_fmt(lin, HEAD_SNAPID), pol_id);

	/*
	 * in the final process, this should be done in the processing of the
	 * blobinfo where if no account is specified, one is selected from the
	 * provider instance list.  For now we will call the selection routing
	 * here.
	 *
	 * NB: The last account parameter is passed as NULL here.  If
	 * we use this interface for sync in the future this should be filled
	 * in with that information
	 */

	/*
	 * NB: the open and close operations should be done in the calling
	 * routine with the policy_provider_info being passed in.  If it is
	 * not passed in, let it be done here.
	 */

	// Access config under read lock
	{
		struct isi_cfm_policy_provider *ppi;
		scoped_ppi_reader ppi_rdr;

		if (!p_policy_provider_info)
		{
			ppi = (isi_cfm_policy_provider *)ppi_rdr.get_ppi(
				&error);
			ON_ISI_ERROR_GOTO(out, error, "failed to get ppi");
		}
		else
		{
			ppi = p_policy_provider_info;
		}

		isi_cfm_get_policy_info(pol_id, ppi, &policy_id,
								&provider_id, &compress, &checksum, &encrypt,
								&archive_snaps, &error);
		if (error)
			goto out;

		// we don't compress files like .zip,.bzip
		if (compress && is_file_compressed(ppi, lin, &error))
			compress = false;
		if (error)
		{
			// In case that the file no longer exist
			// We still need to delete the cloud objects - handled below
			isi_error_free(error);
			error = NULL;
		}

		// TODO: would be good to know the file size here
		////////获得account的信息 isi_cbm_account_util.cpp
		isi_cfm_select_writable_account(ppi, provider_id,
										policy_id, size_constants::SIZE_1GB, account, &error);
		if (error)
			goto out;

		/*
		 * Archives can only happen if the guid is in the permit list
		 * ('false' indicates that it's not ok to archive if the guid
		 * is in the process of being removed)
		 */
		cluster_permission_check(account.account_id.get_cluster_id(),
								 ppi, false, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}
	/////////用account初始化这个智能指针:io_helper,之后这个io_helper处理和云端的io操作
	io_helper.reset(isi_cbm_ioh_creator::get_io_helper(account.type,
													   account));
	if (!io_helper)
	{
		error = isi_cbm_error_new(CBM_INVALID_ACCOUNT,
								  "unsupported account type %d for account id: (%d@%s)",
								  account.type, account.account_id.get_id(),
								  account.account_id.get_cluster_id());
		goto out;
	}
	if (file_was_deleted(lin)) 
	{
		printf("%s called file is deleted\n",__func__);
		// If the file has been deleted and there is a checkpoint
		// data for it we need to clean up the cloud objects based
		// on the mapinfo. Otherwise we will have cloud objects
		// leak.
		//////https://github.west.isilon.com/IME/onefs/commit/85121232559151a49738afa9b260080d28dc09fe
		///////这是一个feature点:当本地的文件已经被删除后,并且存在checkpoint数据(前一次archive时做了一半:pause或者其他原因)
		///////这个时候再一次做了archive, 那么需要删除云端的cloud objects
		if (get_ckpt_data_func != NULL)
		{
			printf("%s called get_ckpt_data_func != NULL\n", __func__);
			get_ckpt_data_func(opaque, &ckpt_data, &ckpt_data_size,
							   &t_state, 0);
			if (ckpt_data_size > 0 && ckpt_data != NULL)
			{
				isi_cbm_archive_ckpt ckpt;
				ckpt.unpack(ckpt_data, ckpt_data_size, &error);
				if (error)
				{
					isi_error_add_context(error,
										  "Failed to unpack mapinfo for Lin %{}",
										  lin_fmt(lin));
					free((void *)ckpt_data);
					ckpt_data = NULL;
					goto out;
				}

				ckpt_mapinfo.copy(ckpt.get_mapinfo(), &error);
				ON_ISI_ERROR_GOTO(out, error, "Failed to copy mapinfo");

				free((void *)ckpt_data);
				printf("%s called try to clear cloud objects and exit\n",__func__);
				clear_cloud_objects(ckpt_mapinfo,
									get_ckpt_data_func,
									set_ckpt_data_func,
									opaque,
									lin, io_helper.get(), &error);
				if (error)
				{
					isi_error_add_context(error,
										  "Cleaning cloud object failed Lin %{}",
										  lin_fmt(lin));
				}
				printf("%s called since file is deleted, do not upload and exit\n", __func__);
				goto out;
			}
		}

		if (!error)
		{
			error = isi_system_error_new(ENOENT, "Lin %{} was deleted",
										 lin_fmt(lin));
		}
		goto out;
	}

	// check the WORM protection
	is_committed = is_cbm_file_worm_committed(lin, HEAD_SNAPID, &error);

	ON_ISI_ERROR_GOTO(out, error, "Failed WORM check for %{}", lin_fmt(lin));

	if (is_committed)
	{
		error = isi_cbm_error_new(CBM_PERM_ERROR,
								  "Lin %{} is WORM committed", lin_fmt(lin));

		goto out;
	}

	// check [other] write restriction
	if (linsnap_is_write_restricted(lin, HEAD_SNAPID))
	{
		error = isi_cbm_error_new(CBM_RESTRICTED_MODIFY,
								  "Restricted file (lin %{} snapid %{})",
								  lin_fmt(lin), snapid_fmt(HEAD_SNAPID));
		goto out;
	}

	isi_cbm_archive_common(lin, policy_id, provider_id,
						   account, io_helper.get(), false/*compress*/, false/*checksum*/, false/*encrypt*/,
						   archive_snaps, get_ckpt_data_func, set_ckpt_data_func,
						   do_async_invalidate, opaque, &error);

out:

	if (error)
	{
		ilog(IL_DEBUG, "Finished archiving %{}, error: %#{}",
			 lin_snapid_fmt(lin, HEAD_SNAPID), isi_error_fmt(error));

		isi_error_handle(error, error_out);
	}
	else
		ilog(IL_DEBUG, "Finished archiving %{} successfully",
			 lin_snapid_fmt(lin, HEAD_SNAPID));
}

/*
 * XXXegc: account should be a policy
 */
static void
isi_cbm_archive_common(ifs_lin_t lin, const isi_cbm_id &policy_id,
					   const isi_cbm_id &provider_id, const isi_cbm_account &account/*account_id, account_key,container_cmo,container_cdo, chunk_size,read_size*/,
					   isi_cbm_ioh_base *ioh, bool compress, bool checksum, bool encrypt,
					   bool archive_snaps,
					   get_ckpt_data_func_t get_ckpt_data_func,
					   set_ckpt_data_func_t set_ckpt_data_func,
					   bool do_async_invalidate,
					   void *opaque,
					   struct isi_error **error_out)
{
	struct isi_cpool_blobinfo blobinfo; //////isi_cbm_write.h中定义 保存account_id,lin,cloud_name(里面有cdo,cmo的container_path),chunk的index,readsize,chunksize信息
	isi_cfm_mapinfo mapinfo(ISI_CFM_MAP_TYPE_OBJECTINFO);
	struct isi_cbm_chunkinfo chunkinfo; //////isi_cbm_write.h中定义
	isi_cpool_master_cdo master_cdo, old_master_cdo; /////传输cdo时,设置两个变量,一个当前正在传输的cdo chunk(master_cdo),一个前一次传输的chunk cdo(old_master_cdo)

	size_t chunksz = account.chunk_size; ///0
	size_t readsz = account.read_size;  ///0
	std::string object;
	u_int64_t filerev;
	u_int64_t ckpt_filerev = 0;
	u_int32_t ckpt_state = CKPT_STATE_NOT_SET;

	struct stat stats;
	bool coi_occured = false;
	bool random_io = false;

	int fd = -1;

	uint64_t index = 0;
	off_t offset = 0;
	off_t total = 0;
	off_t thispass = 0;
	struct isi_error *error = NULL;

	struct isi_cmo_info cmo_info; ////isi_cbm_write.h中定义
	const uint64_t minimum_chunksize = g_minimum_chunksize; // mimimum 8k  chunksize设置为8k
	size_t master_cdo_size = 0;

	scoped_sync_lock sync_lk;
	void *ckpt_data = NULL;
	size_t ckpt_data_size = -1;
	djob_op_job_task_state task_state; /////用在checkpoint中
	isi_cfm_mapinfo ckpt_mapinfo(ISI_CFM_MAP_TYPE_OBJECTINFO);
	bool ckpt_found = false;	  // true means we are continuing interrupted
								  // archiving.
	bool filerev_changed = false; // If true means we have to restart
								  // the archiving
								  // from the beginning since
								  // file rev has changed
								  // and have to clean up all
								  // the CDOs.
	bool resuming = false;

	ASSERT(*error_out == NULL);

	if ((fd = ifs_lin_open(lin, HEAD_SNAPID, O_RDWR)) == -1)
	{
		error = isi_system_error_new(errno, "Opening Lin %{}",
									 lin_fmt(lin));
		goto out;
	}

	if (!archive_snaps)
	{
		bool has_snap = cbm_snap_has_snaps(lin, fd, &error);
		ON_ISI_ERROR_GOTO(out, error);

		if (has_snap)
		{
			error = isi_cbm_error_new(CBM_PERM_ERROR,
									  "The file %{} has snapshots, the policy "
									  "forbids archiving files with snapshots.",
									  lin_fmt(lin));
			goto out;
		}
	}

	/*
	 * We will use the file rev to validate that no changes have been made
	 * to the file while we are writing it out to the cloud.
	 */
	if (getfilerev(fd, &filerev))
	{
		error = isi_system_error_new(errno,
									 "Getting initial filerev for Lin %{}", lin_fmt(lin));
		goto out;
	}

	/*
	 * Get the checkpoint information if there is any.  We need to have
	 * this when we check the stub state since the file could have been
	 * stubbed but the invalidate could have failed.
	 * To protect from change of policy while archiving the compress
	 * and encrypt variables are assigned from the checkpoint and
	 * overriding the policy.
	 */
	//////先注释掉checkpoint代码,jjz
	// if (get_ckpt_data_func != NULL)
	// {
	// 	get_ckpt_data_func(opaque, &ckpt_data, &ckpt_data_size,
	// 					   &task_state, 0);
	// 	printf("%s called get_ckpt_data_func exists and try to get ckpt_data\n", __func__);
	// 	if (ckpt_data_size > 0 && ckpt_data != NULL)
	// 	{
	// 		isi_cbm_archive_ckpt ckpt;
	// 		ckpt.unpack(ckpt_data, ckpt_data_size, &error);
	// 		if (error)
	// 		{
	// 			isi_error_add_context(error,
	// 								  "Failed to unpack mapinfo for Lin %{}",
	// 								  lin_fmt(lin));
	// 			free((void *)ckpt_data);
	// 			ckpt_data = NULL;
	// 			goto out;
	// 		}
	// 		ckpt_mapinfo.copy(ckpt.get_mapinfo(), &error);
	// 		ON_ISI_ERROR_GOTO(out, error, "Failed to copy mapinfo");

	// 		ckpt_filerev = ckpt.get_filerev();
	// 		ckpt_state = ckpt.get_ckpt_state();
	// 		if (ckpt_mapinfo.is_encrypted())
	// 		{
	// 			encrypt = true;
	// 			ckpt_mapinfo.get_encryption_ctx(ectx, &error);
	// 			ON_ISI_ERROR_GOTO(out, error);
	// 		}
	// 		else
	// 		{
	// 			encrypt = false;
	// 		}
	// 		compress = ckpt_mapinfo.is_compressed();

	// 		free((void *)ckpt_data);
	// 		ckpt_found = true;
	// 		ilog(IL_DEBUG, "Checkpoint found from from a previous "
	// 					   "archive for %{} state=%d",
	// 			 lin_fmt(lin), ckpt_state);
	// 		printf("%s called ckpt found from a previous archive\n",__func__);
	// 	}
	// 	else
	// 	{
	// 		printf("%s called ckpt_data == NULL\n", __func__);
	// 	}
	// }

	/*
	 * Get info like the size to chunk out the objects to the cloud.
	 */
	if (fstat(fd, &stats))//////获得上传到云端文件的size
	{
		error = isi_system_error_new(errno,
									 "Getting stat information for Lin %{}", lin_fmt(lin));
		goto out;
	}

	if (stats.st_flags & SF_FILE_STUBBED) ////已经是stubbed文件
	{
		/*
		 * File is marked as a stubbed file, check to see if
		 * the invalidate was performed, if not, clean that up now.
		 */
		if (ckpt_state == CKPT_INVALIDATE_NEEDED)
		{
			printf("%s called archive_ckpt_invalidate\n", __func__);
			archive_ckpt_invalidate(lin, &error);
			goto out;
		}
		else
		{

			error = isi_cbm_error_new(CBM_ALREADY_STUBBED,
									  "Lin %{} is already stubbed", lin_fmt(lin));
			goto out;
		}
	}

	// First acquire the synclock. Try lock.
	sync_lk.lock_ex(fd, false, &error);
	if (error)
	{
		isi_error_add_context(error,
							  "Failed to acquire the sync lock for Lin %{}",
							  lin_fmt(lin));
		goto out;
	}

	// if (ckpt_found && filerev != ckpt_filerev)  ///test jji   不用checkpoint 可以删除
	// {
	// 	filerev_changed = true;
	// 	printf("%s called filerev_changed\n", __func__);
	// }
	/*
	 * Populate the blobinfo with the base information needed for all
	 * objects written for this stub given the acct information and the
	 * lin for the file to be stubbed.
	 */
	/////把account的信息(cdo,cmo的container_path赋值,后续archive,recall需要),lin信息给blobinfo, 填充blobinfo(object_id,container_cmo/cdo)
	isi_cpool_api_get_info(ioh, account.account_id, lin, random_io,
						   blobinfo, &error);
	/*
	 * Checkpoint was found and file hasn't changed.
	 * We need to determine if we are using the same account
	 * as before (meaning there is still space there).
	 * If we are using the same account, than the cloud object
	 * name should be the same as in the checkpoint mapinfo.
	 */
	// if (ckpt_found && filerev == ckpt_filerev)  /////注释掉ckpt jji
	// {
	// 	printf("%s called ckpt_found and filerev == ckpt_filerev\n", __func__);
	// 	isi_cbm_id account_id;
	// 	isi_cloud_object_id obj_base_name;
	// 	std::string container_cdo;

	// 	ilog(IL_DEBUG, "Resuming from a previous archive's checkpoint "
	// 				   "for %{}, object_id: (%s, %ld)",
	// 		 lin_fmt(lin),
	// 		 ckpt_mapinfo.get_object_id().to_c_string(),
	// 		 ckpt_mapinfo.get_object_id().get_snapid());

	// 	get_object_id_from_mapinfo(ckpt_mapinfo, obj_base_name,
	// 							   account_id, container_cdo, &error);
	// 	ON_ISI_ERROR_GOTO(out, error);

	// 	if (account.account_id.equals(account_id))
	// 	{
	// 		blobinfo.cloud_name.obj_base_name = obj_base_name;
	// 		blobinfo.account_id = account_id;
	// 		if (!container_cdo.empty())
	// 		{
	// 			blobinfo.cloud_name.container_cdo =
	// 				container_cdo;
	// 		}
	// 	}
	// 	/*
	// 	 * if current account id not equals to the account id in
	// 	 * checkpoint mapinfo, we should fail this archive job to
	// 	 * avoid one single file be archived by multiple accounts
	// 	 * as defect PSCALE-20693.
	// 	 */
	// 	else
	// 	{
	// 		printf("%s called clear_cloud_objects\n", __func__);
	// 		clear_cloud_objects(ckpt_mapinfo,
	// 							get_ckpt_data_func, set_ckpt_data_func,
	// 							opaque, lin, ioh, &error);
	// 		if (error != NULL)
	// 		{
	// 			isi_error_add_context(error,
	// 								  "Clearing old objects failed during"
	// 								  " aborting of archive due to account"
	// 								  " change Lin %{}",
	// 								  lin_fmt(lin));
	// 			goto out;
	// 		}

	// 		error = isi_cbm_error_new(CBM_NOT_SUPPORTED_ERROR,
	// 								  "CP10 archive does not allow single file to be"
	// 								  " archived to multiple cloud accounts");
	// 		goto out;
	// 	}

	// 	blobinfo.cloud_name.container_cmo =
	// 		ckpt_mapinfo.get_container();
	// 	blobinfo.chunksize = ckpt_mapinfo.get_chunksize();
	// 	blobinfo.readsize = ckpt_mapinfo.get_readsize();
	// 	blobinfo.sparse_resolution = ckpt_mapinfo.get_sparse_resolution();
	// }
	if (1)
	{
		// if the chunksize and readsize is configured, use it
		// for testing purpose.
		if (account.chunk_size > 0)
		{
			blobinfo.chunksize =
				MAX(account.chunk_size, minimum_chunksize);
		}
		if (account.read_size > 0)
		{
			blobinfo.readsize =
				MAX(account.read_size, minimum_chunksize);
		}
		if (account.sparse_resolution > 0)
		{
			blobinfo.sparse_resolution = MIN(
				ROUND_UP_SPA_RESOLUTION(account.sparse_resolution),
				MAX_SPA_RESOLUTION(blobinfo.chunksize));
		}
	}

	// the chunksize is always larger than the readsize, for now
	if (blobinfo.chunksize < blobinfo.readsize)
		blobinfo.chunksize = blobinfo.readsize;
	// make sure the sizes are multiple of 8Ks
	blobinfo.chunksize = (blobinfo.chunksize / minimum_chunksize) * minimum_chunksize;
	blobinfo.readsize = (blobinfo.readsize / minimum_chunksize) * minimum_chunksize;
	if (account.master_cdo_size > 0)
	{
		master_cdo_size = MAX(account.master_cdo_size,
							  minimum_chunksize);
		master_cdo_size = (master_cdo_size / minimum_chunksize) *
						  minimum_chunksize;
	}
	/*
	 * Populate the new mapinfo when the operation is a fresh one.
	 * Otherwise the mapinfo state was assigned from the checkpoint
	 * we will use it to continue the archiving from the place it was
	 * disrrupted.
	 */
	if (!ckpt_found || filerev_changed)
	{
		/*
		 * Record the current file size.  It's okay if it changes
		 * because we catch that at the time that the file is stubbed
		 * since the filerev will have changed
		 */
		mapinfo.set_filesize(stats.st_size);

		mapinfo.set_lin(lin); /* currently only used for error message */

		/*
		 * Set some of the common information for all records in the
		 * map. Set the default chunksize for a map. For some types of
		 * maps this may be the only indication as to where the chunk
		 * boundaries are.
		 */
		if (!policy_id.is_empty())
			mapinfo.set_policy_id(policy_id);
		if (!provider_id.is_empty())
			mapinfo.set_provider_id(provider_id);
		mapinfo.set_chunksize(blobinfo.chunksize);
		blobinfo.readsize = compress ? blobinfo.chunksize : blobinfo.readsize;
		mapinfo.set_readsize(blobinfo.readsize);
		mapinfo.set_sparse_resolution(
			blobinfo.sparse_resolution, blobinfo.chunksize);

		mapinfo.set_account(blobinfo.account_id);  //////gc的时候要用,用来创建io_helper
		mapinfo.set_filesize(stats.st_size);       /////gc需要时需要知道文件length
		mapinfo.set_container(blobinfo.cloud_name.container_cmo);
		///////把object_id写进mapinfo,recall的时候需要通过获得mapinfo来获得object_id,用object_id来下载cdo,cmo
		mapinfo.set_object_id(blobinfo.cloud_name.obj_base_name); 
		mapinfo.set_compression(compress);
		mapinfo.set_checksum(checksum);

		/*
		 * This is the case where we are starting fresh assign offset
		 * index and total to reflect that
		 */
		index = 1;
		offset = 0;
		total = 0;
	}
	// else     ///// 注释ckpt jji
	// { // This is the case where we are recovering from a checkpoint
	// 	// Using the mapinfo get the offset of the file we
	// 	// should start archiving from.
	// 	printf("%s called recover from a ckpt use mapinfo to get the offset of file\n", __func__);
	// 	isi_cfm_mapinfo::iterator itr = ckpt_mapinfo.last(&error);
	// 	ON_ISI_ERROR_GOTO(out, error);

	// 	resuming = true;

	// 	if (itr != ckpt_mapinfo.end())
	// 	{
	// 		const isi_cfm_mapentry *entry = &(itr->second);
	// 		offset = entry->get_offset() + entry->get_length() - 1;
	// 		if (offset < 0)
	// 		{
	// 			error = isi_cbm_error_new(CBM_MAP_INFO_ERROR,
	// 									  "mapinfo contains invalid entry, "
	// 									  "wrong offset: %ld",
	// 									  offset);
	// 			goto out;
	// 		}

	// 		if (entry->get_object_id().get_snapid() == 0)
	// 		{
	// 			// Since we are pre checkpointing we have
	// 			// to rework on the last CDO
	// 			offset = (offset / blobinfo.chunksize) *
	// 					 blobinfo.chunksize;
	// 		}
	// 		else
	// 		{
	// 			// already finished this mcdo, move to the
	// 			// next:
	// 			offset = entry->get_offset() +
	// 					 entry->get_length();
	// 		}

	// 		index = 1 + (offset / blobinfo.chunksize);
	// 		total = offset;
	// 		master_cdo_from_map_entry(old_master_cdo,
	// 								  ckpt_mapinfo, entry);
	// 		// We need to assign the length of the old_master_cdo
	// 		// correctly the length that is in the entry is not correct.
	// 		// In order to do that we are calling get_new_master_cdo and
	// 		// retrieving the length from that
	// 		ioh->get_new_master_cdo(offset,
	// 								old_master_cdo,
	// 								blobinfo.cloud_name, compress,
	// 								checksum, blobinfo.chunksize, blobinfo.readsize,
	// 								blobinfo.sparse_resolution, blobinfo.attr_map,
	// 								master_cdo, master_cdo_size);
	// 		old_master_cdo.set_length(master_cdo.get_length());
	// 	}
	// 	mapinfo.copy(ckpt_mapinfo, &error);
	// 	ON_ISI_ERROR_GOTO(out, error, "Failed to copy mapinfo");

	// 	mapinfo.set_lin(lin); /* currently only used for error message */
	// }

	for (; total < stats.st_size; ++index)
	{
		/////chunksize大小由blobinfo.chunksize, blobinfo.readsize决定
		update_chunkinfo(fd, index, stats.st_size, offset, 0, 0,   //////////////////更新struct isi_cbm_chunk的结构信息
						 compress, checksum, encrypt ? &ectx : NULL, blobinfo,
						 chunkinfo);

		///////主要通过old_master_cdo的index +1 赋值给当前的master_cdo
		ioh->get_new_master_cdo(offset,
								old_master_cdo,
								blobinfo.cloud_name, compress,
								checksum, blobinfo.chunksize, blobinfo.readsize,
								blobinfo.sparse_resolution, blobinfo.attr_map,
								master_cdo, master_cdo_size);

		if (!(old_master_cdo == master_cdo))
		{

			if (old_master_cdo.get_index() != 0 &&
				old_master_cdo.get_snapid() == 0)
			{
				isi_cph_end_write_master_cdo(lin, ioh,
											 old_master_cdo, true, &error);
				if (error)
				{
					isi_error_add_context(error,
										  "Failed to "
										  "isi_cph_end_write_master_cdo "
										  "for lin %{}",
										  lin_fmt(lin));
					goto out;
				}
				if (old_master_cdo.is_concrete())
				{
					update_map_for_master_cdo(mapinfo,
											  old_master_cdo, &error);

					ON_ISI_ERROR_GOTO(out, error,
									  "Failed to update map for master cdo "
									  "for lin: %{}",
									  lin_fmt(lin));
				}
			}

			isi_cph_start_write_master_cdo(lin, ioh, master_cdo,
										   &error);
			if (error)
			{
				isi_error_add_context(error, "Failed to "
											 "isi_cph_start_write_master_cdo "
											 "for lin %{}",
									  lin_fmt(lin));
				goto out;
			}
		}
		// We are saving the updated map to the checkpoint
		// even before we are starting to write the CDO.
		// This is done to prevent a potential leak when
		// The first CDO was written but the checkpointing wasn't
		// and there is a change of filerev that commands
		// cleanup and restart.
		// To remedy when a filerev hasn't occure we will start
		// from an index and offset that is chunksize prior
		// to what the checkpoint map is implying.
		update_map(mapinfo, master_cdo, blobinfo, chunkinfo, &error);//////把master_cdo的index赋值给mapentry的index

		ON_ISI_ERROR_GOTO(out, error, "Failed to update mapinfo");
		printf("%s called after update_map, prepare write_checkpoint\n",__func__);
		write_checkpoint(lin, ioh, mapinfo, filerev,
						 get_ckpt_data_func, set_ckpt_data_func,
						 opaque, CKPT_STATE_NOT_SET, &error);
		if (error != NULL)
		{
			goto out;
		}

		UFAIL_POINT_CODE(fail_archive_no_write, { ASSERT(0); });
		UFAIL_POINT_CODE(cbm_archive_write_nospace, {
			error = isi_cbm_error_new(CBM_LIMIT_EXCEEDED,
									  "Generated by failpoint "
									  "before writing data blob for lin %{}",
									  lin_fmt(lin));
			goto out;
		});
		printf("\n%s called isi_cph_write_data_blob: offset:%ld index:%d\n", __func__, offset, index);
		thispass = isi_cph_write_data_blob(fd, offset, ioh, /////上传每一个chunkinfo.len
										   master_cdo, chunkinfo, &error);
		if (error != NULL)
		{
			isi_error_add_context(error,
								  "failed to write data blob for lin %{}",
								  lin_fmt(lin));
			goto out;
		}
		/////因为当前的这块blob已经上传完成,所以把当前的这块master cdo信息复制给old master cdo(相当于previous cdo)
		old_master_cdo.isi_cpool_copy_master_cdo(
			master_cdo, &error);
		offset += thispass;
		total += thispass;

		UFAIL_POINT_CODE(fail_archive, {if (index > 5) ASSERT(0); });
		/*
		 * For Bug 133722, we need a retryable error (any time) after
		 * the first chunk has been written.
		 */
		UFAIL_POINT_CODE(fail_archive_bug133722, {
			if (index > 1)
			{
				error = isi_system_error_new(EEXIST,
											 "failpoint to test Bug133722");
				goto out;
			}
		});
	}

	if (old_master_cdo.get_index() != 0 &&
		old_master_cdo.get_snapid() == 0)
	{
		isi_cph_end_write_master_cdo(lin, ioh, old_master_cdo,
									 true, &error);

		ON_ISI_ERROR_GOTO(out, error, "Failed to end write master cdo "
									  "for lin: %{}",
						  lin_fmt(lin));

		if (old_master_cdo.is_concrete())
		{
			update_map_for_master_cdo(mapinfo,
									  old_master_cdo, &error);

			if (error)
			{
				isi_error_add_context(error, "Failed to "
											 "update map for master cdo for lin: %{}",
									  lin_fmt(lin));
				goto out;
			}
			printf("%s called prepare write_check\n", __func__);
			write_checkpoint(lin, ioh, mapinfo, filerev,
							 get_ckpt_data_func, set_ckpt_data_func, opaque,
							 CKPT_STATE_NOT_SET, &error);
			if (error != NULL)
			{
				if (isi_cbm_error_is_a(error,
									   CBM_FILE_PAUSED) ||
					isi_cbm_error_is_a(error,
									   CBM_FILE_CANCELLED))
					goto out;
				isi_error_add_context(error, "Failed to "
											 "update_map_checkpoint "
											 "for lin %{}",
									  lin_fmt(lin));
				goto out;
			}
		}
	}

	// Check map content is well formed before we write the CMO
	mapinfo.verify_map(&error);
	ON_ISI_ERROR_GOTO(out, error);

	/////需要将cmo结构体用meta_istream封装上传, class meta_istream : public idata_stream  isi_cbm_stream.h
	////isi_cbm_read.cpp  class cl_cmo_ostream : public isi_cloud::odata_stream cl_cmo_ostream负责下载cmo,解析cmo字段
	cmo_info.version = CPOOL_CMO_VERSION;
	cmo_info.lin = lin;
	cmo_info.stats = &stats;
	cmo_info.mapinfo = &mapinfo;

	if (!resuming || ckpt_mapinfo.get_object_id().get_snapid() == 0)
	{
		ilog(IL_DEBUG, "Writing CMO: (%s, %ld) for file: %{}",
			 blobinfo.cloud_name.obj_base_name.to_c_string(),
			 blobinfo.cloud_name.obj_base_name.get_snapid(),
			 lin_fmt(lin));
		printf("%s called begin isi_cph_write_md_blob\n", __func__);
		isi_cph_write_md_blob(fd, ioh, blobinfo, cmo_info, object,
							  &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	/*
	 * Now that we've stubbed the file, add an entry to the cloud object
	 * index.
	 */
	//////加入到COI中
	isi_cph_add_coi_entry(mapinfo, lin, resuming, &error);
	ON_ISI_ERROR_GOTO(out, error);

	coi_occured = true;

	/*
	 * Update the checkpoint to say that we need to perform the
	 * invalidate.  We could clear this after we see that the stubbing
	 * succeeded successfully, but not necessary as the truncate operation
	 * and adding of the COI are idempodent or at least non-destructive.
	 * If we are not doing a restart then we would not have checkpoint
	 * data and therefore this state would not be set, giving us the
	 * EALREADY response from the kernel when we try to stub again.
	 */
	printf("%s called coi_occured = true\n", __func__);
	write_checkpoint(lin, ioh, mapinfo, filerev,
					 get_ckpt_data_func, set_ckpt_data_func, opaque,
					 CKPT_INVALIDATE_NEEDED, &error);
	if (error != NULL)
	{
		goto out;
	}

	if (isi_cph_verify_stub(fd, filerev))
	{
		isi_cph_create_stub(fd, lin, mapinfo, filerev, ///////把mapinfo信息写入kernel
							do_async_invalidate, &error);
		if (error)
			goto out;
	}

out:

	if (fd != -1)
	{
		sync_lk.unlock();
		close(fd);
		fd = -1;
	}
	// If the file has been deleted and COI was not created than
	// we have a risk of cloud objects leak use the mapinfo
	// to delete all the created cloud objects.
	if (!coi_occured)
	{
		printf("%s called !coi_occured\n",__func__);
		if (file_was_deleted(lin))
		{
			// Since we identified that the error is caused
			// by "normal" FS use case of delete, we free
			// the error, if a new error occures during the clear
			// Archive will be recalled. Otherwise we need to
			// report to task manager that everything went
			// fine and there is no need to reschedule
			// archive.
			isi_error_free(error);
			error = NULL;
			clear_cloud_objects(mapinfo,
								get_ckpt_data_func, set_ckpt_data_func,
								opaque, lin, ioh, &error);
		}
	}

	isi_error_handle(error, error_out);
}

/**
 * Generate the information needed to describe the information being accessed,
 * the chunk.
 *
 * fd of file
 * chunk index - beginning with 1
 * total file size
 * offset in the file to access the data
 * offset in the object to place data (currently always doing full chunk)
 * transfer size if less than full chunk or to eof.
 * information about the target cloud provider needed to formulate chunk
 * chunkinfo - description of the data transfer
 */
////////blobinfo.chunksize 1M, blobinfo.readsize 128K是根据/isi_cloud_api/cl_provider.h中额默认配置
/////需要将blobinfo的配置信息应用到真实一个上传文件时需要的chunkinfo
static void  
update_chunkinfo(int fd, uint64_t index, size_t fsize, off_t src_offset,
				 off_t dst_offset, size_t transfer_size, bool compress, bool checksum,
				 encrypt_ctx_p ectx, const struct isi_cpool_blobinfo &blobinfo,
				 struct isi_cbm_chunkinfo &chunkinfo)
{
	chunkinfo.fd = fd;
	chunkinfo.index = index;
	chunkinfo.file_offset = src_offset;
	chunkinfo.object_offset = dst_offset;
	//MIN((fsize - src_offset), 400);
	chunkinfo.len = MIN((fsize - src_offset), blobinfo.chunksize);
	if (transfer_size > 0)
		chunkinfo.len = MIN(transfer_size, chunkinfo.len);

	chunkinfo.compress = compress;
	chunkinfo.checksum = checksum;
	chunkinfo.ectx = ectx;
}

static bool
isi_cph_verify_stub(int fd, u_int64_t filerev)
{
	return true;
}

static void
isi_cph_queue_invalidation(ifs_lin_t lin,
						   const struct timeval &delayed_inval, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	isi_cbm_csi_sptr cs;
	// Let's first schedule the truncation.

	cs = isi_cbm_get_csi(&error);
	ON_ISI_ERROR_GOTO(out, error);

	// to queue cached file for invalidation
	cs->add_cached_file(lin, HEAD_SNAPID, delayed_inval, &error);
	ON_ISI_ERROR_GOTO(out, error);
out:
	isi_error_handle(error, error_out);
}

void isi_cph_create_stub(int fd, ifs_lin_t lin,
						 isi_cfm_mapinfo &mapinfo, u_int64_t filerev,
						 bool do_async_invalidate,
						 struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct timeval delayed_inval = {0};
	struct timeval tv = {0};

	bool to_delay_trunc = false;
	scoped_stub_lock stub_lock;

	ASSERT(*error_out == NULL);
#if 0
	/*
	 * XXXegc: debugging code
	 */
	mapinfo.dump();
	/*
	 * XXXegc: end of debug code
	 */
#endif
	gettimeofday(&delayed_inval, NULL);
	tv = delayed_inval;

	isi_cbm_file_get_settle_time(lin, HEAD_SNAPID,
								 mapinfo.get_policy_id(), STT_DELAYED_INVALIDATE, delayed_inval,
								 &error);
	ON_ISI_ERROR_GOTO(out, error);

	to_delay_trunc = (delayed_inval.tv_sec > tv.tv_sec);

	if (to_delay_trunc)
	{
		isi_cph_queue_invalidation(lin, delayed_inval, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	stub_lock.lock_ex(fd, false, &error);
	ON_ISI_ERROR_GOTO(out, error);

	mapinfo.write_map(fd, filerev, false, to_delay_trunc, &error);

	if (do_async_invalidate && error &&
		isi_cbm_error_is_a(error, CBM_STUB_INVALIDATE_ERROR))
	{
		struct stat stats;

		if (fstat(fd, &stats))
		{
			ilog(IL_DEBUG, "Could not get stats for file %{}, "
						   "error no: %d",
				 lin_fmt(lin), errno);
			goto out;
		}

		ilog(IL_DEBUG, "Invalidation failed, but the file is already"
					   "a stub: %{}. Will do invalidation asynchronously. "
					   "Original error: %#{}",
			 lin_fmt(lin), isi_error_fmt(error));

		if (stats.st_flags & SF_FILE_STUBBED)
		{
			struct isi_error *error2 = NULL;
			isi_cph_queue_invalidation(lin, delayed_inval, &error2);

			if (error2)
			{
				ilog(IL_ERR, "Failed to queue an asynchronous"
							 " invalidation for file %{}. Error : %#{}",
					 lin_fmt(lin), isi_error_fmt(error2));
				isi_error_free(error2);
				error2 = NULL;
				goto out; // retain the original error
			}

			// invalidation queued successfully
			isi_error_free(error);
			error = NULL;
		}
	}
out:
	if (error)
		isi_error_handle(error, error_out);
}

/**
 * Get the object Id from mapping info.
 */
static void
get_object_id_from_mapinfo(const isi_cfm_mapinfo &mapinfo,
						   isi_cloud_object_id &object_id, isi_cbm_id &account_id,
						   std::string &container_cdo, struct isi_error **error_out)
{
	isi_cfm_mapinfo::map_iterator itr = mapinfo.last(error_out);

	if (*error_out)
		return;

	if (itr == mapinfo.end())
	{
		object_id = mapinfo.get_object_id();
		account_id = mapinfo.get_account();
	}
	else
	{
		object_id = itr->second.get_object_id();
		account_id = itr->second.get_account_id();
		container_cdo = itr->second.get_container();
	}
}

static void
clear_cloud_objects(isi_cfm_mapinfo &mapinfo,
					get_ckpt_data_func_t get_ckpt_data_func,
					set_ckpt_data_func_t set_ckpt_data_func,
					void *opaque,
					ifs_lin_t lin, isi_cbm_ioh_base *ioh, isi_error **error_out)
{
	isi_error *error = NULL;
	isi_cbm_coi_sptr coi;

	cl_object_name cloud_name;

	std::auto_ptr<coi_entry> coi_entry;
	cloud_name.obj_base_name = mapinfo.get_object_id();
	cloud_name.container_cmo = mapinfo.get_container();

	coi = isi_cbm_get_coi(&error);
	ON_ISI_ERROR_GOTO(out, error);

	coi_entry.reset(coi->get_entry(mapinfo.get_object_id(), &error));

	if (error && isi_system_error_is_a(error, ENOENT))//////正常情况下archive没成功,所以不会有coi_entry生成,自然这里获取不到coi
	{
		printf("%s called prepare cloud_gc\n", __func__);
		isi_error_free(error);
		error = NULL;
		scoped_coid_lock coid_lock(*coi, mapinfo.get_object_id());
		coid_lock.lock(true, &error);
		ON_ISI_ERROR_GOTO(out, error);

		// The object has not made to the COI yet, collect them
		// manually
		isi_cbm_remove_cloud_objects(mapinfo, NULL, NULL,
									 cloud_name, ioh, false,
									 get_ckpt_data_func, set_ckpt_data_func,
									 opaque, &error);
		ON_ISI_ERROR_GOTO(out, error);

		coi->remove_object(mapinfo.get_object_id(), true, &error);
		ON_ISI_ERROR_GOTO(out, error);

		goto out;
	}

	ON_ISI_ERROR_GOTO(out, error);

	// Otherwise. we have made it to the COI, let regular GC handle it.
	// That honors the references by other objects
	{
		struct timeval dod = {0};
		coi->remove_ref(mapinfo.get_object_id(), lin, HEAD_SNAPID,
						dod, NULL, &error);
		ON_ISI_ERROR_GOTO(out, error,
						  "failed to remove reference to object ID %{} "
						  "by LIN/snapID %{}",
						  isi_cloud_object_id_fmt(&mapinfo.get_object_id()),
						  lin_snapid_fmt(lin, HEAD_SNAPID));
	}
out:
	isi_error_handle(error, error_out);
}

static bool
file_was_deleted(ifs_lin_t lin)
{
	off_t len = PATH_MAX + 1;
	size_t zero = 0;
	size_t sz = 0;
	char *buf = NULL;

	/*
	 * XXXegc: need to be able to have a predictable way to specify length
	 * of metadata so that we can use the read stream for this.  It seems
	 * that we need to prespecify the length of the data for the current
	 * implementation so this hack is here for now...
	 */
	buf = (char *)malloc(PATH_MAX + 1);

	ASSERT(buf);
	errno = 0;

	if (pctl2_lin_get_path_plus(ROOT_LIN, lin, HEAD_SNAPID, 0, 0,
								ENC_DEFAULT, len, buf, &sz, 0, NULL, &zero, 0, NULL, NULL,
								PCTL2_LIN_GET_PATH_NO_PERM_CHECK))
	{ // bug 220379
		if (errno == ENOENT)
		{
			free(buf);
			return true;
		}
	}
	free(buf);
	return false;
}

static void
update_map(isi_cfm_mapinfo &mapinfo,
		   const isi_cpool_master_cdo &master_cdo,
		   const struct isi_cpool_blobinfo &blobinfo,
		   const struct isi_cbm_chunkinfo &chunkinfo,
		   struct isi_error **error_out)
{
	isi_cfm_mapentry *pmapentry = new isi_cfm_mapentry; /////isi_cbm_mapentry.h
	ASSERT(pmapentry);

	isi_cloud_object_id objectid = blobinfo.cloud_name.obj_base_name;
	objectid.set_snapid(master_cdo.get_snapid());
	////设置account_id(用来产生cdo_io_helper),chunkinfo.file_offset, 这个offset在isi_cbm_gc.cpp中用到,用来获得index = mapentry->get_offset() / chunksize
	pmapentry->setentry(CPOOL_CFM_MAP_VERSION, blobinfo.account_id,
						chunkinfo.file_offset, chunkinfo.len, blobinfo.cloud_name.container_cdo,
						objectid);

	pmapentry->set_index(master_cdo.get_index());

	mapinfo.add(*pmapentry, error_out);

	delete pmapentry;
}

static void
write_checkpoint(ifs_lin_t lin,
				 isi_cbm_ioh_base *ioh,
				 isi_cfm_mapinfo &mapinfo,
				 uint64_t filerev,
				 get_ckpt_data_func_t get_ckpt_data_func,
				 set_ckpt_data_func_t set_ckpt_data_func,
				 void *opaque,
				 u_int32_t ckpt_state,
				 struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	djob_op_job_task_state task_state = DJOB_OJT_NONE;
	if (set_ckpt_data_func != NULL)
	{
		printf("%s called set_ckpt_data_func != NULL\n", __func__);
		isi_cbm_archive_ckpt ckpt(filerev, 0, mapinfo, &error);

		ON_ISI_ERROR_GOTO(out, error,
						  "Failed to construct archive checkpoint object");

		ckpt.set_ckpt_state(ckpt_state);
		ckpt.pack(&error);
		ON_ISI_ERROR_GOTO(out, error, "Failed to pack the checkpoint");

		set_ckpt_data_func(opaque, ckpt.get_serialized_ckpt(),
						   ckpt.get_packed_size(),
						   &task_state, 0); ///offset = 0;
		if (task_state == DJOB_OJT_PAUSED)
		{
			error = isi_cbm_error_new(CBM_FILE_PAUSED,
									  "Archive Paused for file %{}",
									  lin_fmt(lin));
			printf("%s called set_ckpt_data_func task_state == DJOB_OJT_PAUSED exit\n", __func__);
			goto out;
		}
		if (task_state == DJOB_OJT_CANCELLED)
		{
			clear_cloud_objects(mapinfo,
								get_ckpt_data_func, set_ckpt_data_func,
								opaque, lin, ioh, &error);
			if (error == NULL)
			{
				error = isi_cbm_error_new(CBM_FILE_CANCELLED,
										  "Archive Canceled for file %{}",
										  lin_fmt(lin));
			}
			printf("%s called set_ckpt_data_func task_state == DJOB_OJT_CANCELLED clear_cloud_objects exit\n", __func__);
			goto out;
		}
		if (task_state == DJOB_OJT_STOPPED)
		{
			printf("%s called set_ckpt_data_func task_state == DJOB_OJT_STOPPED exit\n", __func__);
			error = isi_cbm_error_new(CBM_FILE_STOPPED,
									  "Archive Stopped for file %{}",
									  lin_fmt(lin));
			goto out;
		}
	}
out:
	isi_error_handle(error, error_out);
}
static void
update_map_for_master_cdo(isi_cfm_mapinfo &mapinfo,
						  isi_cpool_master_cdo &master_cdo, struct isi_error **error_out)
{
	isi_cfm_mapinfo::iterator itr;
	mapinfo.get_containing_map_iterator_for_offset(
		itr, master_cdo.get_offset(), error_out);

	if (*error_out)
	{
		isi_error_add_context(*error_out,
							  "Failed to find entry by offset: %ld",
							  master_cdo.get_offset());
		return;
	}

	ASSERT(itr != mapinfo.end()); // it must be found

	isi_cloud_object_id objectid = itr->second.get_object_id();
	objectid.set_snapid(master_cdo.get_snapid());

	// clone it first:
	isi_cfm_mapentry mapentry(itr->second);
	mapentry.set_object_id(objectid);
	mapentry.set_plength(master_cdo.get_physical_length());
	mapinfo.add(mapentry, error_out);
}

static bool
is_file_compressed(struct isi_cfm_policy_provider *ppi, ifs_lin_t lin,
				   struct isi_error **error_out)
{
	char *path = NULL;
	size_t path_sz = 0;
	bool is_compressed = false;
	struct isi_error *error = NULL;
	int err = lin_get_path(0, lin, HEAD_SNAPID, 0, 0, &path_sz, &path);

	if (err)
	{
		error = isi_system_error_new(errno,
									 "lin_get_path error for Lin %{}", lin_fmt(lin));
		goto out;
	}
	else
	{
		const char *lst = cpool_get_compressed_file_ext(
			ppi->cp_context);
		char *ext = strrchr(path, '.');

		if (ext == NULL)
			goto out;

		while (lst != NULL && *lst != '\0')
		{
			char *nxt = strchr(lst, ' ');

			if (nxt &&
				int(strlen(ext)) == (nxt - lst) &&
				strncmp(lst, ext, nxt - lst) == 0)
				is_compressed = true;

			if (!nxt && strcmp(lst, ext) == 0)
				is_compressed = true;

			if (is_compressed || !nxt)
				break;

			lst = nxt + 1;
		}
	}

out:
	isi_error_handle(error, error_out);

	if (path != NULL)
		free(path);

	return is_compressed;
}
