/**
 * Scoped Policy-Provider-Info(ppi) reader and writer.
 *
 * These contructs provide thread-safe access to the CBM config.  
 */
#ifndef _ISI_CBM_SCOPED_PPI_H_
#define _ISI_CBM_SCOPED_PPI_H_

#include <memory>
#include <isi_util_cpp/scoped_rwlock.h>

// Many threads can simultaneously instantiate scoped_ppi_reader and use
// it to read config.  Instances of scoped_ppi_reader can be nested. 
class scoped_ppi_reader
{
public:
	scoped_ppi_reader();
	~scoped_ppi_reader();

	const struct isi_cfm_policy_provider * get_ppi(
	    struct isi_error **error_out) const;

private:
	// not to be implemented
	scoped_ppi_reader(const scoped_ppi_reader &);
	scoped_ppi_reader operator=(const scoped_ppi_reader &);

	scoped_rdlock rdl_;

};


// Only one thread can instantiate a scoped_ppi_writer and use it to write(and
// read) config; all other instantiatiations of scoped_ppi_writer/reader
// are blocked pending the destruction of the aforementioned.  Instances of
// scoped_ppi_writer cannot be nested with each other or with instances of
// scoped_ppi_reader.
class scoped_ppi_writer
{
public:
	scoped_ppi_writer();
	~scoped_ppi_writer();

	struct isi_cfm_policy_provider * get_ppi(
	    struct isi_error **error_out);

	/**
	 * Commit the modified config; Once called, 'ppi' cannot
	 * be used and the caller must obtain another instance
	 * by calling get_ppi().
	*/
	void commit_cpool(struct isi_cfm_policy_provider *ppi,
	    struct isi_error **error_out);
	void commit_smartpools(struct isi_cfm_policy_provider *ppi,
	    struct isi_error **error_out);
private:
	// not to be implemented
	scoped_ppi_writer(const scoped_ppi_writer &);
	scoped_ppi_writer operator=(const scoped_ppi_writer &);

	scoped_wrlock  wrl_;
};


// Smart pointer versions of ppi reader and writer.  Ownership can be
// transferred across scopes.  XXX use unique_ptr when we move to C++11.
typedef std::auto_ptr<scoped_ppi_reader> ptr_ppi_reader;
typedef std::auto_ptr<scoped_ppi_writer> ptr_ppi_writer;

ptr_ppi_reader
get_ppi_reader(struct isi_error **error_out);

ptr_ppi_writer
get_ppi_writer(struct isi_error **error_out);


// XXXPD Move to // lib/isi_cpool_config
/**
 * Check whether the error is a cpool-commit-retryable error
 */
bool cpool_commit_retryable(struct isi_error *error);
// XXXPD Move to // lib/isi_pools_fsa_config
/**
 * Check whether the error is a smartpools-commit-retryable error
 */
bool smartpools_commit_retryable(struct isi_error *error);


/**
 * Clear out and release resources help by the current config.
 */
void close_ppi(void);

#endif // _ISI_CBM_SCOPED_PPI_H_

