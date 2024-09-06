#include <isi_util/isi_error.h>
#include <isi_util_cpp/scoped_lock.h>
#include <isi_cpool_cbm/isi_cbm_policyinfo.h>

#include "isi_cbm_scoped_ppi.h"
#include </usr/include/pthread.h>

namespace {
#if 0
const pthread_rwlockattr_t *
get_write_pref_rwattr(void)
{
	static pthread_rwlockattr_t attr;
	int ret = 0;

	ret = pthread_rwlockattr_setkind_np(attr,
	    PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);

	ASSERT(ret == 0);

	return &attr;
}

/**
 * The ppi rwlock used by scoped_ppi_reader/writer.
 * 
 * We want an updated config (on-disk) to be available (in-memory)
 * as quickly as possible, hence we give users of scoped_ppi_writer
 * preference over scoped_ppi_reader
 */
pthread_rwlock_obj the_ppi_lock(get_write_pref_rwattr());
#else
/**
 * We do not have support for pthread_rwlockattr setting in
 * our current BSD libthr distro.  However the current impl gives writers
 * preference by default so creating the lock as shown below
 */
pthread_rwlock_obj the_ppi_lock;
#endif

class mgd_ppi // used for controlled access to ppi
{
public:
	mgd_ppi() {}

	static struct isi_cfm_policy_provider * get_ppi(struct isi_error **);
	static void destroy_ppi(struct isi_error **);

private:
	~mgd_ppi();

	// not to be implemented
	mgd_ppi(const mgd_ppi &);
	mgd_ppi & operator=(const mgd_ppi &);

	static struct isi_cfm_policy_provider ppi_;
	static scoped_mutex ppi_mutex_;  // for ppi creation and access
	static bool ppi_inited_;
};

struct isi_cfm_policy_provider mgd_ppi::ppi_;
scoped_mutex mgd_ppi::ppi_mutex_;
bool mgd_ppi::ppi_inited_;


// can be called by a reader or writer
struct isi_cfm_policy_provider *
mgd_ppi::get_ppi(struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	scoped_lock lock(*ppi_mutex_);

	struct isi_cfm_policy_provider *ppi = NULL;

	if (ppi_inited_) {
		ppi = &ppi_;
		goto out;
	}

	// load policy_provider
	isi_cfm_open_policy_provider_info(&ppi_, &error);
	if (error) {
		goto out;
	}

	ppi_inited_ = true;
	ppi = &ppi_;

 out:
	isi_error_handle(error, error_out);
	return ppi;
}

// should only be called by a writer
void
mgd_ppi::destroy_ppi(struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	scoped_lock lock(*ppi_mutex_);

	if (!ppi_inited_)
		goto out;

	// unload policy provider
	isi_cfm_close_policy_provider_info(&ppi_, &error);
	if (error) {
		goto out;
	}

	ppi_inited_ = false;
 out:
	isi_error_handle(error, error_out);
}

} // namespace

void
close_ppi(void)
{
	struct isi_error *error = NULL;

	scoped_ppi_writer writer; 	// to ensure exlusive access

	mgd_ppi::destroy_ppi(&error);
	if (error)
		isi_error_free(error);
}

scoped_ppi_reader::scoped_ppi_reader():
	rdl_(the_ppi_lock)
{
}

scoped_ppi_reader::~scoped_ppi_reader()
{
}

const struct isi_cfm_policy_provider *
scoped_ppi_reader::get_ppi( struct isi_error **error_out) const
{
	struct isi_error *error = NULL;
	struct isi_cfm_policy_provider *ppi = NULL;

	ppi =  mgd_ppi::get_ppi(&error);
	if (error)
		goto out;

 out:
	isi_error_handle(error,error_out);
	return  ppi;
}


scoped_ppi_writer::scoped_ppi_writer():
	wrl_(the_ppi_lock)
{
}

scoped_ppi_writer::~scoped_ppi_writer()
{
}

struct isi_cfm_policy_provider *
scoped_ppi_writer::get_ppi( struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct isi_cfm_policy_provider *ppi = NULL;

	ppi =  mgd_ppi::get_ppi(&error);
	if (error)
		goto out;

 out:
	isi_error_handle(error,error_out);
	return  ppi;
}


// XXXPD Move to // lib/isi_cpool_config
bool cpool_commit_retryable(struct isi_error *error)
{

	ASSERT(error);

	// We follow gconfig_helpers.h and
	// treat all GCI_COMMIT_ERROR_CLASS as retryable
	return isi_error_is_a(error, GCI_COMMIT_ERROR_CLASS);
}

// XXXPD Move to // lib/isi_pools_fsa_config
bool smartpools_commit_retryable(struct isi_error *error)
{
	ASSERT(error);

	// We follow gconfig_helpers.h and
	// treat all GCI_COMMIT_ERROR_CLASS as retryable
	return isi_error_is_a(error, GCI_COMMIT_ERROR_CLASS);
}


void
scoped_ppi_writer::commit_cpool(struct isi_cfm_policy_provider *ppi,
	    struct isi_error **error_out)
{
	struct isi_error *error = NULL, *error2 = NULL;
	struct cpool_config_context *ctx = NULL;

	ASSERT(ppi);
	ASSERT(ppi->cp_context);
	ASSERT(error_out);

	ctx = ppi->cp_context;
	cpool_context_commit(ctx, &error);
	if (error)
		goto out;

 out:
	mgd_ppi::destroy_ppi(&error2);
	if (error2) {
		if (error) {
			const char *msg;

			msg = isi_error_get_message(error2);
			isi_error_add_context(error, "%s", msg);
			isi_error_free(error2);
		} else {
			error = error2;
		}
		error2 = NULL;
	}
	isi_error_handle(error, error_out);
}

void
scoped_ppi_writer::commit_smartpools(struct isi_cfm_policy_provider *ppi,
	    struct isi_error **error_out)
{
	struct isi_error *error = NULL, *error2 = NULL;
	struct smartpools_context *ctx = NULL;

	ASSERT(ppi);
	ASSERT(ppi->sp_context);
	ASSERT(error_out);

	ctx = ppi->sp_context;
	smartpools_save_all_policies(ctx, &error);
	if (error) {
		isi_error_add_context(error,
		    "Failed to save all policies");
		goto out;
	}

	smartpools_commit_changes(ctx, &error); 
	if (error) {
		isi_error_add_context(error,
		    "Failed to commit policy changes");
		goto out;
	}

  out:
	mgd_ppi::destroy_ppi(&error2);
	if (error2) {
		if (error) {
			const char *msg;

			msg = isi_error_get_message(error2);
			isi_error_add_context(error, "%s", msg);
			isi_error_free(error2);
		} else {
			error = error2;
		}
		error2 = NULL;
	}
	isi_error_handle(error, error_out);
}

ptr_ppi_reader
get_ppi_reader(struct isi_error **error_out)
{
	scoped_ppi_reader *spr = NULL;

	spr = new scoped_ppi_reader();

	return ptr_ppi_reader(spr);
}


ptr_ppi_writer
get_ppi_writer(struct isi_error **error_out)
{
	scoped_ppi_writer *spw = NULL;

	spw = new scoped_ppi_writer();

	return ptr_ppi_writer(spw);
}
