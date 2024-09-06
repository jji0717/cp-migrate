#include "isi_cbm_util.h"

#include <isi_util/isi_error.h>
#include <isi_domain/dom.h>

#include "isi_cbm_cache.h"
#include "isi_cbm_file.h"
#include "isi_cbm_mapper.h"
#include "isi_cbm_scoped_ppi.h"
#include "isi_cbm_error.h"
#include "isi_cbm_policyinfo.h"

/** 
 * Allow for range locking of open file specifying RD or WR (exclusive) 
 * and whether lock operation should block;
 */
bool
isi_cpool_lock_file(int fd, bool write, off_t offset, size_t len, bool block)
{
	struct flock lockthis;

	lockthis.l_start = offset;
	lockthis.l_len = len;
	lockthis.l_type = (write? F_WRLCK : F_RDLCK);
	lockthis.l_whence = SEEK_SET;
	lockthis.l_sysid = 0;

	return !fcntl(fd, (block ? F_SETLKW : F_SETLK), &lockthis);
}

bool
isi_cpool_unlock_file(int fd, off_t offset, size_t len)
{
	struct flock lockthis;

	lockthis.l_start = offset;
	lockthis.l_len = len;
	lockthis.l_type = F_UNLCK;
	lockthis.l_whence = SEEK_SET;
	lockthis.l_sysid = 0;

	return !fcntl(fd, F_SETLK, &lockthis);
}


static void
copy_content(int src_fd, int tgt_fd, isi_error **error_out)
{
	isi_error *error = NULL;
	const int buffer_size = 4 * 8192;
	char buffer[buffer_size];

	while (true) {
		ssize_t read_len = read(src_fd, buffer, buffer_size);

		if (read_len > 0) {
			ssize_t wrt_len = write(tgt_fd, buffer, read_len);

			if (wrt_len != read_len) {
				error = isi_system_error_new(errno,
				    "Could not write target, %ld %ld",
				    wrt_len, read_len);
				goto out;
			}
		}
		else if(read_len == 0) {
			break;
		} else {
			error = isi_system_error_new(errno,
			    "Could not read source, %ld", read_len);
			goto out;
		}
	}
 out:
	isi_error_handle(error, error_out);
}

/**
 * This utility copy a stub file from src to tgt
 */
void
isi_cbm_copy_stub_file(const char *src, const char *tgt, isi_error **error_out)
{
	int err = 0;
	int src_fd = -1, tgt_fd = -1;
	int src_ads_dir_fd = -1, tgt_ads_dir_fd = -1;
	int src_ads_fd = -1, tgt_ads_fd = -1;
	isi_error *error = NULL;
	isi_cbm_coi_sptr coi;

	struct stat stat;
	isi_cfm_mapinfo mapinfo;
	uint64_t filerev = 0;

	src_fd = open(src, O_RDONLY);
	if (src_fd < 0) {
		error = isi_system_error_new(errno, "Could not open %s",
		    src);
		goto out;
	}
	tgt_fd = open(tgt, O_TRUNC | O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	if (tgt_fd < 0) {
		error = isi_system_error_new(errno, "Could not open %s", tgt);
		goto out;
	}

	err = fstat(src_fd, &stat);
	if (err) {
		error = isi_system_error_new(errno, "Could not stat %s", src);
		goto out;
	}

	err = ftruncate(tgt_fd, stat.st_size);
	if (err) {
		error = isi_system_error_new(errno, "Could not truncate %s",
		    tgt);
		goto out;
	}

	err = fstat(tgt_fd, &stat);
	if (err) {
		error = isi_system_error_new(errno, "Could not stat %s", tgt);
		goto out;
	}

	// now copy the mapping info
	isi_cph_get_stubmap(src_fd, mapinfo, &error);

	ON_ISI_ERROR_GOTO(out, error);

	err = fsync(tgt_fd);
	if (err) {
		error = isi_system_error_new(errno, "Could not fsync %s",
		    tgt);
		goto out;
	}

	getfilerev(tgt_fd, &filerev);

	err = fstat(tgt_fd, &stat);
	if (err) {
		error = isi_system_error_new(errno, "Could not fstat %s",
		    tgt);
		goto out;
	}

	mapinfo.set_lin(stat.st_ino);

	coi = isi_cbm_get_coi(&error);
	ON_ISI_ERROR_GOTO(out, error);

	// add the reference LIN
	coi->add_ref(mapinfo.get_object_id(), stat.st_ino, &error);
	ON_ISI_ERROR_GOTO(out, error);

	mapinfo.write_map(tgt_fd, filerev, false, false, &error);
	ON_ISI_ERROR_GOTO(out, error);

	src_ads_dir_fd = enc_openat(src_fd, "." , ENC_DEFAULT,
	    O_RDONLY|O_XATTR);

	if (src_ads_dir_fd < 0) {
		error = isi_system_error_new(errno,
		    "Could not open ADS dir for %s", src);
		goto out;
	}

	src_ads_fd = enc_openat(src_ads_dir_fd, ISI_CBM_CACHE_CACHEINFO_NAME,
	    ENC_DEFAULT, O_RDONLY);

	if (src_ads_fd < 0) {
		if (errno == ENOENT) {
			// cache has not yet been populated.
			// skip and copy mapping info
			goto out;
		}
		error = isi_system_error_new(errno,
		    "Could not open ADS %s for %s", ISI_CBM_CACHE_CACHEINFO_NAME,
		    src);
		goto out;
	}

	tgt_ads_dir_fd = enc_openat(src_fd, "." , ENC_DEFAULT,
	    O_RDONLY|O_XATTR);

	if (tgt_ads_dir_fd < 0) {
		error = isi_system_error_new(errno,
		    "Could not open ADS dir for %s", tgt);
		goto out;
	}

	tgt_ads_fd = enc_openat(tgt_ads_dir_fd, ISI_CBM_CACHE_CACHEINFO_NAME,
	    ENC_DEFAULT,  O_CREAT | O_WRONLY);

	if (tgt_ads_fd < 0) {
		error = isi_system_error_new(errno,
		    "Could not create ADS %s for %s",
		    ISI_CBM_CACHE_CACHEINFO_NAME, tgt);
		goto out;
	}

	copy_content(src_fd, tgt_fd, &error);
	ON_ISI_ERROR_GOTO(out, error);

	// copy the cache info ADS
	copy_content(src_ads_fd, tgt_ads_fd, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	if (src_fd >= 0)
		close(src_fd);
	if (tgt_fd >= 0)
		close(tgt_fd);
	if (src_ads_dir_fd >= 0)
		close(src_ads_dir_fd);
	if (src_ads_fd >= 0)
		close(src_ads_fd);

	if (tgt_ads_dir_fd >= 0)
		close(tgt_ads_dir_fd);
	if (tgt_ads_fd >= 0)
		close(tgt_ads_fd);

	isi_error_handle(error, error_out);
}

static void
process_time_fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args,
    union fmt_conv_arg arg)
{
	ASSERT(fmt != NULL);
	ASSERT(args != NULL);

	time_t process_time = static_cast<time_t>(arg.uint64);
	struct tm timeinfo_utc, timeinfo_local;
	char timebuf_utc[80], timebuf_local[80];

	gmtime_r(&process_time, &timeinfo_utc);
	strftime(timebuf_utc, sizeof(timebuf_utc),
	    "%a %b %d %H:%M:%S %Y UTC", &timeinfo_utc);

	localtime_r(&process_time, &timeinfo_local);
	strftime(timebuf_local, sizeof(timebuf_local),
	    "%a %b %d %H:%M:%S %Y %Z", &timeinfo_local);

	fmt_print(fmt, "%s/%s", timebuf_utc, timebuf_local);
}

struct fmt_conv_ctx
process_time_fmt(const struct timeval &process_time)
{
	time_t process_time_sec = process_time.tv_sec;
	struct fmt_conv_ctx ctx = { process_time_fmt_conv,
	    { uint64: process_time_sec } };
	return ctx;
}

static const char*
guid_state_to_string(enum cm_attr_state state)
{
	switch (state) {
	    case CM_A_ST_DISABLED:
		return "disabled";

	    case CM_A_ST_ENABLED:
		return "enabled";

	    case CM_A_ST_ADDING:
		return "adding";

	    case CM_A_ST_REMOVING:
		return "removing";
	}

	ASSERT(false, "Unexpected cm_attr_state value: %d", (int)state);
	return "unexpected";
}

void
cluster_permission_check(const char *cluster_id,
    const isi_cfm_policy_provider *ppi, bool ok_if_guid_being_removed,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct permit_guid_list_element *guid_el = NULL;
	enum cm_attr_state state = CM_A_ST_DISABLED;

	if (!ppi) {
		scoped_ppi_reader ppi_reader;

		ppi = ppi_reader.get_ppi(&error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	guid_el = smartpools_get_permit_list_element_for_guid(ppi->sp_context,
	    cluster_id);

	/* Note - a NULL guid_el means guid is implicitly disabled */
	if (guid_el != NULL)
		state = smartpools_get_guid_element_state(guid_el);

	/*
	 * Permission is disallowed unless (a) the guid is explicitly enabled or
	 * (b) the guid is being removed and we are accepting guids in the
	 * removing state
	 */
	if (state != CM_A_ST_ENABLED &&
	    (!ok_if_guid_being_removed || state != CM_A_ST_REMOVING)) {
		error = isi_cbm_error_new(CBM_NOT_IN_PERMIT_LIST,
		    "No permission to access cluster %s : state is \"%s\"",
		    cluster_id, guid_state_to_string(state));
	}

 out:
	isi_error_handle(error, error_out);
}

void
cluster_permission_check(struct isi_cbm_file *file_obj,
    const isi_cfm_policy_provider *ppi, bool ok_if_guid_being_removed,
    struct isi_error **error_out)
{

	cluster_permission_check(
	    file_obj->mapinfo->get_account().get_cluster_id(), ppi,
	    ok_if_guid_being_removed, error_out);
}

/**
 * Check if we have a valid lin at HEAD but in a write-restricted domain.
 * Such a lin (unlike one in a snapshot or a worm domain)
 * may become unrestricted at a later time.
 */
bool
linsnap_is_write_restricted(ifs_lin_t lin, ifs_snapid_t snapid)
{
	int i = 0;
	u_int dom_set_size = 0;
	struct isi_error *error = NULL;
	struct domain_set dom_set;
	struct domain_entry dom_entry;
	ifs_domainid_t cur_id;
	bool inited = false, restricted = false;

	if (lin == INVALID_LIN || snapid != HEAD_SNAPID)
		goto out;

	domain_set_init(&dom_set);
	inited = true;

	if (dom_get_info_by_lin(lin, snapid, &dom_set, NULL, NULL))
		ON_SYSERR_GOTO(out, errno, error,
		    "Could not get domain info for %{}",
		    lin_snapid_fmt(lin, snapid));


	dom_set_size = domain_set_size(&dom_set);
	for (i = 0; i < dom_set_size; i++) {
		cur_id = domain_set_atindex(&dom_set, i);
		if (dom_get_entry(cur_id, &dom_entry) != 0)
			continue;
		if (dom_entry.d_flags & DOM_RESTRICTED_WRITE) {
			restricted = true;
			goto out;
		}
	}

 out:
	if (error) {
		 ilog(IL_ERR, "%#{}", isi_error_fmt(error));
		 isi_error_free(error);
	}

	if (inited)
		domain_set_clean(&dom_set);

	return restricted;
}
