#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <ifs/ifm_adt.h>

#include <isi_ufp/isi_ufp.h>
#include <isi_util/isi_assert.h>
#include <isi_util/isi_buf_istream.h>
#include <isi_util/isi_extattr.h>
#include <ifs/ifs_lin_open.h>
#include <isi_domain/dom.h>

#include "isi_migrate/migr/isirep.h"
#include "pworker.h"


// Add a hardlink path to a fmt string to be added to an isi_error.

static void
dmk_add_link_to_err_msg(struct migr_pworker_ctx *pw_ctx, 
    struct migr_dirent *md, const struct parent_hash_entry *pentry,     
    struct fmt *err_fmt, struct isi_error **error_out)
{
	int ret = -1;
	int dfd = -1;

	char *abspath = NULL;
	size_t abspathlen = 0;


	static const char *LOGLINE_FMT_HDR = "\t\"/ifs/";
	static const size_t LOGLINE_FMT_HDR_LEN = 7;

	static const char *LOGLINE_FMT_FTR = "\"";
	static const size_t LOGLINE_FMT_FTR_LEN = 1;

	unsigned int num_hashes = 0;
	unsigned int i = 0;

	struct isi_error *error = NULL;


	log(TRACE, "%s(%llx)", __func__, md->stat->st_ino);

	ASSERT(pw_ctx && md && err_fmt && pentry);

	dfd = ifs_lin_open(pentry->lin, HEAD_SNAPID, O_RDONLY);

	if (dfd < 0) {
		error = isi_system_error_new(errno,
		    "%s could not open parent LIN %llx for %s", __func__,
		    pentry->lin, FNAME(md));
		goto out;
	}

	num_hashes = uint_vec_size(&pentry->hash_vec);

	for (i = 0; i < num_hashes; i++) {
		ret = lin_get_path(0, md->stat->st_ino, HEAD_SNAPID, 
		    pentry->lin, pentry->hash_vec.uints[i], &abspathlen, 
		    &abspath);
		if (ret || !abspath) {
			error = isi_system_error_new(errno,
			    "%s could not get absolute path for hardlink to %s",
			    __func__, FNAME(md));
			goto out;
		}

		fmt_append(err_fmt, LOGLINE_FMT_HDR, 
		    LOGLINE_FMT_HDR + LOGLINE_FMT_HDR_LEN);
		fmt_append(err_fmt, abspath, abspath + abspathlen);
		fmt_append(err_fmt, LOGLINE_FMT_FTR, 
		    LOGLINE_FMT_FTR + LOGLINE_FMT_FTR_LEN);

		free(abspath);
		abspath = NULL;
	}
	
out:
	if (abspath)
		free(abspath);

    	if (dfd > 0)
		close(dfd);

	isi_error_handle(error, error_out);
}

// Get parent hashes for LIN. Slower than get_parent_lins(), below, but 
// contains name hashes for all hardlinks to a LIN in a parent dir.
// Essentially a wrapper call to isi_extattr_get_fd().

static void
get_parent_hashes(const ifs_lin_t lin, struct parent_hash_vec *parents,
    struct isi_error **error_out)
{
	struct isi_extattr_blob eb = {};
	struct isi_buf_istream bis = {};
	int fd = -1;
	int ret = -1;

	struct isi_error *error = NULL;

	static const size_t DEF_EXTATTR_BLOB_SIZE = 8192;

	fd = ifs_lin_open(lin, HEAD_SNAPID, O_RDONLY);
	if (fd < 0) {
		error = isi_system_error_new(errno,
		    "%s could not open LIN %llx", __func__, lin);
		goto out;
	}

	eb.data = malloc(DEF_EXTATTR_BLOB_SIZE);
	ASSERT(eb.data);

	eb.allocated = DEF_EXTATTR_BLOB_SIZE;

	ret = isi_extattr_get_fd(fd, EXTATTR_NAMESPACE_IFS, "parent_hash_vec", 
	    &eb);
	if (ret) {
		error = isi_system_error_new(errno,
		    "%s could not get parent hashes for LIN %llx", __func__,
		    lin);
		goto out;
	}

		
	parent_hash_vec_clean(parents);
	isi_buf_istream_init(&bis, eb.data, eb.used);
	if (!parent_hash_vec_decode(parents, &bis.super)) {
		error = isi_system_error_new(errno,
		    "%s could not decode parent hashes for LIN %llx", __func__,
		    lin);
		goto out;
	}

out:

	if (fd != -1)
		close(fd);

	free(eb.data);

	isi_error_handle(error, error_out);
}



// Wrapper call to pctl2_lin_get_parents(). 
// Allocates entries to initial size specified by expected_entries. Returns
// entries/count as out params. **Caller responsible for freeing entries.**

static void
get_parent_lins(const ifs_lin_t lin, const size_t expected_parents, 
    struct parent_entry **parents, unsigned int *num_parents_out, 
    struct isi_error **error_out)
{
	int ret = -1;
	unsigned int parents_size = expected_parents;

	struct isi_error *error = NULL;

	ASSERT(!(*parents));

	*num_parents_out = expected_parents;

	while (!(*parents)) {
		*parents = malloc(sizeof(struct parent_entry) *
		    parents_size);
		ASSERT(*parents);

		parents_size = *num_parents_out;

		ret = pctl2_lin_get_parents(lin, HEAD_SNAPID,
		    *parents, parents_size, num_parents_out);
		if (ret) {
		       if (errno != ENOSPC) {
			       	error = isi_system_error_new(errno, 
				    "%s failed to get parent LINs for %llx",
				    __func__, lin);
				goto out;
		       }
		} else {			
			break;
		}

		free(*parents);
		*parents = NULL;
	}					

out:

	isi_error_handle(error, error_out);
}

// Uptreewalk to determine if a directory is a subdir of the given domain
// root LIN.
static bool
dir_in_domain(const ifs_lin_t dlin, const ifs_lin_t domain_root, 
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	ifs_lin_t cur_lin = dlin;

	int ret = -1;
	int dfd = -1;
	struct stat st;

	struct parent_entry *parents = NULL;
	unsigned int num_parents = 0;

	bool res = false;

	ASSERT(domain_root);
	log(TRACE, "%s(%llx, %lld)", __func__, dlin, domain_root);

	while (cur_lin != ROOT_LIN && cur_lin != domain_root) {

		dfd = ifs_lin_open(cur_lin, HEAD_SNAPID, O_RDONLY);
		if (dfd == -1) {
			error = isi_system_error_new(errno, 
			    "%s could not open %llx", __func__, cur_lin);
			goto out;
		}

		ret = fstat(dfd, &st);
		if (ret == -1) {
			error = isi_system_error_new(errno, 
			    "%s could not stat %llx", __func__, cur_lin);
			goto out;
		}

		ASSERT(S_ISDIR(st.st_mode));
	
		get_parent_lins(cur_lin, st.st_nlink, &parents, &num_parents, 
		    &error);

		if (error)
			goto out;
	
		ASSERT(num_parents == 1);

		cur_lin = parents[0].lin;

		close(dfd);
		free(parents);
		parents = NULL;
		num_parents = 0;
	}

	if (cur_lin == domain_root) {
		res = true;
		goto out;
	}


out:
	if (dfd > 0)
		close(dfd);

	if (parents)
		free(parents);

	isi_error_handle(error, error_out);

	return res;
}

static void
dmk_check_process_hardlink(struct migr_pworker_ctx *pw_ctx,
    struct migr_dirent *md, struct isi_error **error_out)
{
	int i = 0;

	int ret = -1;

	bool found_blocking_hardlink = false;
	struct parent_hash_vec PARENT_HASH_VEC_INIT_CLEAN(parents);
	struct domain_entry dom_entry = {};
	unsigned int num_parents = 0;

	bool in_dom = false;

	struct fmt FMT_INIT_CLEAN(fmt);
	
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);


	ASSERT(md && md->stat);
	ASSERT(md->stat->st_nlink >= 1);

	if (md->stat->st_nlink == 1) 
		goto out;

	ret = dom_get_entry(pw_ctx->domain_id, &dom_entry);
	if (ret == -1) {
		error = isi_system_error_new(errno,
		    "%s could not get domain entry for %lld", 
		    __func__, pw_ctx->domain_id);
		goto out;
	}

	get_parent_hashes(md->stat->st_ino, &parents, &error);
	if (error)
		goto out;

	num_parents = parent_hash_vec_size(&parents);

	for (i = 0; i < num_parents; i++) {
		in_dom = dir_in_domain(parents.parents[i].lin, dom_entry.d_root,
		    &error);
		if (error)
			goto out;

		if (!in_dom) {
			if (!found_blocking_hardlink) {
				fmt_print(&fmt, "Found hardlinks to %s outside "
				    "of the SyncIQ policy source root path. "
				    "Failing job. To continue, remove the "
				    "following links to %s:\n", 
				    FNAME(md), FNAME(md));

				found_blocking_hardlink = true;
			}

			dmk_add_link_to_err_msg(pw_ctx, md,
			    &parents.parents[i], &fmt, &error);
			if (error)
				goto out;

		}
	}

	if (found_blocking_hardlink) {
		error = isi_siq_error_new(E_SIQ_DOMAIN_MARK, "%s",
		    fmt_string(&fmt));
		goto out;
	}

out:
	isi_error_handle(error, error_out);
}

// Manually mark lins, in case domain mark bulk op fails.
static void
dmk_mark_lins(struct migr_pworker_ctx *pw_ctx, struct isi_error **error_out) 
{
	int ret = -1;

	struct isi_error *error = NULL;

	size_t i = 0;
	size_t num_lins = lin_set_size(&pw_ctx->domain_mark_lins);

	log(TRACE, "%s", __func__);

	for (i = 0; i < num_lins; i++) {
		ret = ifs_domain_add_bylin(pw_ctx->domain_mark_lins.lins[i],
		    pw_ctx->domain_id);
		if (ret && errno != EEXIST && errno != ENOENT) {
			error = isi_system_error_new(errno, 
			    "Failed to mark domain on LIN (%llx)", 
			    pw_ctx->domain_mark_lins.lins[i]);
			goto out;
		}
	}

out:
	isi_error_handle(error, error_out);	
}

static void
dmk_flush_lin_queue(struct migr_pworker_ctx *pw_ctx, 
    struct isi_error **error_out)
{
	int ret = -1;
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	if (lin_set_size(&pw_ctx->domain_mark_lins) < 1) {
		goto out;
	}

	ret = ifs_domain_lin_bulk_op(pw_ctx->domain_mark_lins.lins, 
	    lin_set_size(&pw_ctx->domain_mark_lins), pw_ctx->domain_id, true);
	if (ret) {
		if (errno != EEXIST && errno != ENOENT) {
			system("sysctl efs.dexitcode_ring "
			    ">>/var/crash/dexitcode_bug171787.txt");
			error = isi_system_error_new(errno, 
			    "%s could not flush LIN queue", __func__);
			goto out;
		}

		log(DEBUG, "Domain bulk mark op failed. Setting manually.");
		dmk_mark_lins(pw_ctx, &error);
		if (error)
			goto out;
	}

out:
	lin_set_truncate(&pw_ctx->domain_mark_lins);
	isi_error_handle(error, error_out);

}

static void
dmk_queue_lin(struct migr_pworker_ctx *pw_ctx, ifs_lin_t lin, 
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	log(TRACE, "%s(%llx)", __func__, lin);

	UFAIL_POINT_CODE(dmk_lin_die,
	    if (RETURN_VALUE == (lin & 0xFFFFFFFF)) {
	    	error = isi_system_error_new(EDOOFUS, "Failpoint triggered "
		    "for LIN %llx", lin);
		goto out;
	    });

	lin_set_add(&pw_ctx->domain_mark_lins, lin);
	if (lin_set_size(&pw_ctx->domain_mark_lins) >= DOM_BULK_MAX_OPS) {
		dmk_flush_lin_queue(pw_ctx, &error);
		if (error)
			goto out;
	}
out:
	isi_error_handle(error, error_out);

}

static enum migr_dir_result
dmk_handle_dir(struct migr_pworker_ctx *pw_ctx, struct migr_dirent *md, 
    struct isi_error **error_out)
{
	ASSERT(pw_ctx->outstanding_acks == 0);
	throttle_delay(pw_ctx);
	return migr_handle_generic_dir(pw_ctx, &pw_ctx->dir_state, md, 
	    dmk_start_work, error_out);
}

static enum file_type
dmk_handle_file(struct migr_pworker_ctx *pw_ctx, struct migr_dirent *md, 
    struct isi_error **error_out)
{
	struct stat *st = NULL;
	struct isi_error *error = NULL;
	bool need_unref = true;
	struct migr_dir_state *dir = &pw_ctx->dir_state;
	
	st = md->stat;
	throttle_delay(pw_ctx);

	log(TRACE, "%s(%s)", __func__, FNAME(md));

	dmk_check_process_hardlink(pw_ctx, md, &error);
	if (error)
		goto out;

	dmk_queue_lin(pw_ctx, md->stat->st_ino, &error);
	if (error)
		goto out;

	pw_ctx->tw_cur_stats->files->total++;
	
out:
	if (need_unref && md && dir->dir)
		migr_dir_unref(dir->dir, md);

	isi_error_handle(error, error_out);
	return MIGR_CONTINUE_FILE;
}

static enum migr_dir_result
dmk_continue_dir(struct migr_pworker_ctx *pw_ctx, 
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct migr_dirent *md = NULL;
	enum migr_dir_result result = MIGR_CONTINUE_DIR;
	enum file_type type;

	log(TRACE, "%s", __func__);

	md = migr_dir_read(pw_ctx->dir_state.dir, &error);
	if (error) {
		goto out;
	} else if (!md) {
		result = MIGR_CONTINUE_DONE;
		goto out;
	}

	type = dirent_type_to_file_type(md->dirent->d_type);

	log(TRACE, "%s read %s %s", __func__, file_type_to_name(type),
	    FNAME(md));

	if (type == SIQ_FT_DIR)
		result = dmk_handle_dir(pw_ctx, md, &error);
	else
		result = dmk_handle_file(pw_ctx, md, &error);

out:
	isi_error_handle(error, error_out);
	return result;
}

static void
dmk_finish_reg_dir(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	log(TRACE, "%s", __func__);

	ASSERT(pw_ctx->outstanding_acks == 0);
	ASSERT(pw_ctx->work->rt == WORK_DOMAIN_MARK);
	ASSERT(!DIFF_SYNC_BUT_NOT_ADS(pw_ctx));
	ASSERT(!pw_ctx->stf_upgrade_sync);

	dmk_flush_lin_queue(pw_ctx, &error);
	if (error)
		goto out;

	migr_finish_generic_reg_dir(pw_ctx, dmk_start_work, &error);
	if (error)
		goto out;

out:
	isi_error_handle(error, error_out);
}

void
dmk_continue_reg_dir(struct migr_pworker_ctx *pw_ctx,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	enum migr_dir_result result;
	bool finished = false;
	struct migr_dir_state *dir = &pw_ctx->dir_state;

	log(TRACE, "%s", __func__);

	result = dmk_continue_dir(pw_ctx, &error);
	if (error)
		goto out;

	log(TRACE, "%s: dmk_continue_reg_dir() returns %s",
	    __func__, migr_dir_result_str(result));

	switch (result) {

	case MIGR_CONTINUE_DIR:
	case MIGR_CONTINUE_FILE:
		break;

	case MIGR_CONTINUE_DONE:
		if (migr_dir_is_end(dir->dir) &&
		    !(dir->dir->_stat.st_flags & UF_ADS))
			pw_ctx->tw_cur_stats->dirs->src->visited++;

		finished = true;
		break;

	default:		
		ASSERT(!"Unexpected MIGR_CONTINUE_ADS (or invalid) return!");
		break;
	}

	if (finished)
		dmk_finish_reg_dir(pw_ctx, &error);
	if (error)
		goto out;
out:
	isi_error_handle(error, error_out);
}

void
dmk_start_work(struct migr_pworker_ctx *pw_ctx, 
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct migr_dir *dir;
	int ret;

	log(TRACE, "%s", __func__);

	pw_ctx->domain_mark = true;

	ASSERT(!pw_ctx->dir_state.dir);

	siq_track_op_beg(SIQ_OP_DIR_FINISH);

	dir = migr_tw_get_dir(&pw_ctx->treewalk, &error);
	if (error) 
		goto out;

	dir->visits++;
	log(TRACE, "%s: Dir %s visits=%d", __func__, dir->name, dir->visits);
	pw_ctx->dir_state.dir = dir;
	if (fchdir(migr_dir_get_fd(dir)) < 0) {
		error = isi_system_error_new(errno,
		    "Failed to chdir in %s: %s", __func__,
		    pw_ctx->treewalk._path.path);
		goto out;

	}

	ret = 0;
	if (!ret)
		ret = fstat(migr_dir_get_fd(dir), &pw_ctx->dir_state.st);
	if (ret < 0) {
		error = isi_system_error_new(errno,
		    "Failed to fstat in %s: %s", __func__,
		    pw_ctx->treewalk._path.path);
	}

	ret = ifs_domain_allowwrite(pw_ctx->domain_id, 
	    pw_ctx->domain_generation);
	if (ret == -1) {
		error = isi_system_error_new(errno,
		    "Could not get write permission for domain");
		goto out;
	}

	dmk_queue_lin(pw_ctx, pw_ctx->dir_state.st.st_ino, &error);
	if (error)
		goto out;

	migr_call_next(pw_ctx, dmk_continue_reg_dir);
out:
	isi_error_handle(error, error_out);
}


