#ifndef __ISI_CBM_ERROR_UTIL__H__
#define __ISI_CBM_ERROR_UTIL__H__

#include <sys/isi_celog_eventids.h>
#include <isi_ufp/isi_ufp.h>

#include <isi_cpool_config/cpool_config.h>
#include <isi_cloud_api/isi_cloud_api.h>
#include "isi_cbm_error.h"
#include "isi_cbm_error_protected.h"
#include "isi_cbm_id.h"


struct cpool_events_attrs {
	ifs_lin_t lin;
	off_t offset;
	isi_cbm_id account_id;
	std::string stub_entityname;
	std::string filename;
};


#define CATCH_CLAPI_EXCEPTION(cev_attrs, error, send_cev)				\
	catch (isi_cloud::cl_exception &cl_err) {			\
		error = convert_cl_exception_to_cbm_error(cl_err);	\
	}								\
	catch (isi_exception &e) {					\
		error = e.get()? e.detach() :				\
		    isi_cbm_error_new(					\
		        CBM_ISI_EXCEPTION_FAILURE,"%s", e.what());	\
	}								\
	catch (const std::exception &e) {				\
		error = isi_cbm_error_new(				\
		    CBM_STD_EXCEPTION_FAILURE, "%s", e.what());		\
	}                                                               \
    if (send_cev) \
    { \
        send_cloudpools_event(cev_attrs, error, __FUNCTION__); \
    }

/**
 * Converts cl_exception to cbm error
 */
struct isi_error *convert_cl_exception_to_cbm_error(
    isi_cloud::cl_exception &cl_err);

/**
 * Routine to send cloudpool events to CELOG.
 */
void send_cloudpools_event(struct cpool_events_attrs *cev_attrs,
    struct isi_error *incoming_error, char const *caller);


/**
 * Routine to set cloudpool event parameters.
 */
void set_cpool_events_attrs(struct cpool_events_attrs *cev_attrs,
    ifs_lin_t lin, off_t offset, isi_cbm_id account_id, 
    std::string stub_entityname);

/**
 * Trigger the cloudpools fail point for sending events.
 */
void trigger_failpoint_cloudpool_events();


#endif // __ISI_CBM_ERROR_UTIL__H__
