#include "isi_cpool_cbm/isi_cbm_write.h"
#include <sys/stat.h>

#include <ifs/ifs_types.h>
#include "ifs/ifs_syscalls.h"
#include "ifs/bam/bam_pctl.h"
#include <isi_pools_fsa_gcfg/smartpools_util.h>
#include <isi_cpool_config/cpool_config.h>
#include "isi_cpool_cbm/isi_cbm_error.h"
#include "isi_cpool_cbm/isi_cbm_error_util.h"
#include "isi_cpool_cbm/isi_cbm_file.h"
#include "isi_cpool_cbm/isi_cbm_policyinfo.h"
#include "isi_cpool_cbm/isi_cbm_scoped_coid_lock.h"
#include "isi_cpool_cbm/isi_cbm_stream.h"
#include "isi_cpool_cbm/isi_cbm_sparse.h"
#include "io_helper/encrypted_istream.h"
#include "io_helper/isi_cbm_ioh_base.h"

using namespace isi_cloud;

/*
 * XXXegc: My expectation is that this is a POC only routine.  I would expect
 * that the most of the work being done here would be provider specific and
 * would be handed off the the provider layer.  While the get_provider
 * interface does some of this today, the following questions remain:
 *
 * How is the container determined (hash for RAN, specifed for some other
 * providers)?
 *
 * How is object overwrite projection handled (some will not allow
 * overwrite, some will, how do we protect destroying data for something
 * else)?
 *
 * The questions already handled here (max size, chunk size, etc.)
 */
void //////填充blobinfo
isi_cpool_api_get_info(isi_cbm_ioh_base *ioh, const isi_cbm_id &account_id,
    ifs_lin_t lin, bool random_io, struct isi_cpool_blobinfo &blobinfo,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	ioh->generate_cloud_name(lin, blobinfo.cloud_name);////////赋值object_id, container_cmo, container_cdo

	// for now we will just take the account info
	blobinfo.chunksize = ioh->get_chunk_size(random_io);
	blobinfo.readsize = ioh->get_read_size();
	blobinfo.account_id = account_id;
	blobinfo.sparse_resolution = ioh->get_sparse_resolution();

	isi_error_handle(error, error_out);
}

void
isi_cfm_get_policy_info(uint32_t pol_id,
    struct isi_cfm_policy_provider *ppi,
    isi_cbm_id *ppolicy_id, isi_cbm_id *pprovider_id,
    bool *compress, bool *checksum, bool *encrypt,
    bool *archive_snaps, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct fp_policy *policy = NULL;

	ASSERT( ppi && error_out);

	/*
	 * Get policy id to provider id translation
	 */
	smartpools_get_policy_by_id(pol_id, &policy,
	    ppi->sp_context, &error);

	// XXXegc: For now, policy == NULL is not found, will have
	// isi error set in future
	if (!policy && !error) {
		error = isi_system_error_new(ENOENT,
		    "Policy id %d was not found and no error was returned",
		    pol_id);
		goto out;
	}
	if (error)
		goto out;

	if (ppolicy_id != NULL)
		ppolicy_id->set_from_policy(*policy);

	if (pprovider_id) {
		pprovider_id->set_provider_from_policy(*policy);
	}

	if (compress != NULL) {
		*compress = policy->attributes->cloudpool_action
		    ->cloud_compression_enabled;
	}

	if (encrypt != NULL) {
		*encrypt = policy->attributes->cloudpool_action
		    ->cloud_encryption_enabled;
	}

	if (archive_snaps != NULL) {
		*archive_snaps = policy->attributes->cloudpool_action
		    ->archive_snapshot_files;

	}

	if (checksum != NULL)
		*checksum = ppi->cp_context->cfg->checksum;

out:
	isi_error_handle(error, error_out);

}

void
isi_cph_write_md_blob(int fd, isi_cbm_ioh_base *ioh,
    struct isi_cpool_blobinfo &blobinfo, struct isi_cmo_info &cmo_info,
    std::string &objectname, struct isi_error **error_out)
{
	meta_istream strm;
	size_t len = PATH_MAX;
	int ret = 0;
	struct isi_error *error = NULL;
	struct fmt FMT_INIT_CLEAN(meta_info);
	char *buf = NULL;
	cpool_events_attrs *cev_attrs = NULL;
	ASSERT(*error_out == NULL);

	/*
	 * XXXegc: need to be able to have a predictable way to specify length
	 * of metadata so that we can use the read stream for this.  It seems
	 * that we need to prespecify the length of the data for the current
	 * implementation so this hack is here for now...
	 */

	do {
		buf = (char *) realloc(buf, len+1);
		ASSERT(buf);

		ret = pctl2_lin_get_path_plus(ROOT_LIN, cmo_info.lin,
		    HEAD_SNAPID, 0, 0, ENC_DEFAULT, len + 1, buf, &len,
		    0, NULL, NULL, 0, NULL, NULL,
		    PCTL2_LIN_GET_PATH_NO_PERM_CHECK);  // bug 220379
	} while(ret && errno == ENOSPC);

	// if (ret) {
	// 	ON_SYSERR_GOTO(out, errno, error,
	// 	    "Cannot obtain path for lin %{}",
	// 	    lin_fmt(cmo_info.lin));
	// }

	fmt_print(&meta_info, "%{} %s\n", lin_fmt(cmo_info.lin), (char *)buf);
    std::string str((char*)fmt_string(&meta_info));  ////:   /ifs/f1
	//std::string str;

	strm.setvers(cmo_info.version);
	strm.setmd((char *)fmt_string(&meta_info));///////cl_cmo_ostream.get_path()
	//std::string mi("str"); ///((char*)fmt_string(&meta_info));
	// char* ptr = (char*)fmt_string(&meta_info);
	// printf("ptr:%c",*ptr);
	strm.setmi(*(cmo_info.mapinfo));
	strm.setst(*(cmo_info.stats));

	/* end of XXXegc */

	cev_attrs = new cpool_events_attrs;

	try {
		std::string entityname;
		entityname = get_entityname(blobinfo.cloud_name.container_cmo,
		    ioh->get_cmo_name(blobinfo.cloud_name.obj_base_name));

		set_cpool_events_attrs(cev_attrs, cmo_info.lin, -1,
		    ioh->get_account().account_id, entityname);

		ioh->write_cmo(blobinfo.cloud_name,
		    blobinfo.attr_map, strm.length(), strm, true,
		    cmo_info.mapinfo->is_compressed(),
		    cmo_info.mapinfo->has_checksum());

		trigger_failpoint_cloudpool_events();
	}
	CATCH_CLAPI_EXCEPTION(cev_attrs, error, true);
	delete cev_attrs;

	if (buf)
		free(buf);

 out:
	isi_error_handle(error, error_out);
}

ssize_t
isi_cph_write_data_blob(int fd, off_t offset, isi_cbm_ioh_base *ioh,
    isi_cpool_master_cdo &master_cdo,
    struct isi_cbm_chunkinfo &chunkinfo,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	data_istream	strm;
	off_t		set_pos;
	struct stat	st;
	ifs_lin_t	lin = 0;
	cl_object_name	cloud_object_name;
	std::string	entityname;
	cpool_events_attrs *cev_attrs = NULL;

	ASSERT(*error_out == NULL);

	strm.setfd(fd);
	set_pos = strm.setoffset(offset);
	if (set_pos < 0) {
		error = isi_system_error_new(errno,
		    "Cannot set offset for fd %d",
		    fd);
		goto out;
	}

	if (fstat(fd, &st) < 0) {
		error = isi_system_error_new(errno,
		    "Cannot stat fd %d",
		    fd);
		goto out;
	}

	lin = st.st_ino;

	isi_cbm_build_sparse_area_map(lin, st.st_snapid, offset, chunkinfo.len,
	    master_cdo.get_chunksize(), master_cdo.get_sparse_area_header_ptr(),
	    &error);
	ON_ISI_ERROR_GOTO(out, error);

	cev_attrs = new cpool_events_attrs;
	try {
		cloud_object_name = master_cdo.get_cloud_object_name();
		std::string cdo_name =
		    ioh->get_cdo_name(cloud_object_name.obj_base_name,
		    chunkinfo.index);
		entityname = get_entityname(cloud_object_name.container_cdo,
		    cdo_name);

		set_cpool_events_attrs(cev_attrs, lin, offset,
		    ioh->get_account().account_id, entityname);

		ioh->write_cdo(master_cdo,
		    chunkinfo.index,
		    chunkinfo.len, strm, true, master_cdo.is_concrete(),
		    chunkinfo.compress, chunkinfo.checksum, chunkinfo.ectx);

		trigger_failpoint_cloudpool_events();
	}
	CATCH_CLAPI_EXCEPTION(cev_attrs, error, true)
out:
	if (cev_attrs != NULL) {
		delete cev_attrs;
	}
	isi_error_handle(error, error_out);

	return chunkinfo.len;
}

void
isi_cph_start_write_master_cdo(ifs_lin_t lin, isi_cbm_ioh_base *ioh,
    isi_cpool_master_cdo &master_cdo, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	cl_object_name cloud_object_name;
	std::string entityname;

	cpool_events_attrs *cev_attrs = new cpool_events_attrs;

	try {
		cloud_object_name = master_cdo.get_cloud_object_name();
		entityname = get_entityname(cloud_object_name.container_cdo,
		    ioh->get_cdo_name(
		    cloud_object_name.obj_base_name,
		    master_cdo.get_index()));

		set_cpool_events_attrs(cev_attrs, lin,
		    master_cdo.get_offset(), ioh->get_account().account_id,
		    entityname);

		ioh->start_write_master_cdo(master_cdo);

		trigger_failpoint_cloudpool_events();
	}
	CATCH_CLAPI_EXCEPTION(cev_attrs, error, true)
	delete cev_attrs;

	isi_error_handle(error, error_out);
}


void
isi_cph_end_write_master_cdo(ifs_lin_t lin, isi_cbm_ioh_base *ioh,
    isi_cpool_master_cdo &master_cdo, bool commit,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	cl_object_name cloud_object_name;
	std::string entityname;
	cpool_events_attrs *cev_attrs = new cpool_events_attrs;

	try {
		cloud_object_name = master_cdo.get_cloud_object_name();
		entityname = get_entityname(cloud_object_name.container_cdo,
		    ioh->get_cdo_name(
		    cloud_object_name.obj_base_name,
		    master_cdo.get_index()));

		set_cpool_events_attrs(cev_attrs, lin,
		    master_cdo.get_offset(), ioh->get_account().account_id,
		    entityname);

		ioh->end_write_master_cdo(master_cdo, commit);

		trigger_failpoint_cloudpool_events();
	}
	CATCH_CLAPI_EXCEPTION(cev_attrs, error, true)
	delete cev_attrs;

	isi_error_handle(error, error_out);
}

void
isi_cph_add_coi_entry(isi_cfm_mapinfo &mapinfo, ifs_lin_t lin,
    bool ignore_exist,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	isi_cbm_coi_sptr coi;
	uint8_t node_id[IFSCONFIG_GUID_SIZE] = {0};

	coi = isi_cbm_get_coi(&error);
	ON_ISI_ERROR_GOTO(out, error);

	{
		scoped_coid_lock coid_lock(*coi, mapinfo.get_object_id());
		coid_lock.lock(true, &error);
		ON_ISI_ERROR_GOTO(out, error);

		// TODO: should add_ref take a uint8_t pointer instead?
		mapinfo.get_account().get_cluster_id(node_id);
		coi->add_object(mapinfo.get_object_id(), node_id,
		    mapinfo.get_account(), mapinfo.get_container(),
		    mapinfo.is_compressed(), mapinfo.has_checksum(), lin,
		    &error);
		if (error != NULL) {
			if (isi_system_error_is_a(error, EEXIST) &&
			    ignore_exist) {
				ilog(IL_DEBUG, "%{} has already has its "
				    "COI entry added: (%s, %ld). error: %#{}",
				    lin_fmt(lin),
				    mapinfo.get_object_id().to_c_string(),
				    mapinfo.get_object_id().get_snapid(),
				    isi_error_fmt(error));
				isi_error_free(error);
				error = NULL;
			} else {
				isi_error_add_context(error,
				    "Failed to coi::add_object for %{}",
				    lin_fmt(lin));
				goto out;
			}
		}
	}

 out:
	isi_error_handle(error, error_out);
}
