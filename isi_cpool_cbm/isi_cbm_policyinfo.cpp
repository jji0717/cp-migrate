#include <isi_ufp/isi_ufp.h>

#include "isi_cbm_policyinfo.h"

/*
 * Failpoints
 *
 * (ENOTSOCK is used to identify the injected failures as it is very unlikely
 * to occur organically.)
 */
#define UFP_OPEN_PPI_SMARTPOOLS_FAILURE \
	UFAIL_POINT_CODE(isi_cfm_open_ppinfo__sp_open_failure, {	\
		if (error == NULL) {					\
			smartpools_close_context(&ppi->sp_context,	\
			    &error);					\
			if (error == NULL) {				\
				ppi->sp_context = NULL;			\
				error = isi_system_error_new(ENOTSOCK,	\
				    "user failpoint hit for "		\
				    "open smartpools context failure");	\
			}						\
		}							\
	});
#define UFP_OPEN_PPI_CLOUDPOOLS_FAILURE \
	UFAIL_POINT_CODE(isi_cfm_open_ppinfo__cp_open_failure, {	\
		if (error == NULL) {					\
			cpool_context_close(ppi->cp_context);		\
			ppi->cp_context = NULL;				\
			error = isi_system_error_new(ENOTSOCK,		\
			    "user failpoint hit "			\
			    "for open cloudpools context failure");	\
		}							\
	});
#define UFP_OPEN_PPI_PROXY_FAILURE \
	UFAIL_POINT_CODE(isi_cfm_open_ppinfo__proxy_open_failure, {	\
		if (error == NULL) {					\
			isi_proxy_context_close(&ppi->proxy_context,	\
			    &error);					\
			if (error == NULL) {				\
				ppi->proxy_context = NULL;		\
				error = isi_system_error_new(ENOTSOCK,	\
				    "user failpoint hit for "		\
				    "open proxy context failure");	\
			}						\
		}							\
	});

/*
 * using the policy name provided, determine the information needed to
 * represent the data in the a stub map.
 */
void
isi_cfm_open_policy_provider_info(struct isi_cfm_policy_provider *ppi,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	ASSERT(ppi);

	ppi->sp_context = NULL;
	ppi->cp_context = NULL;
	ppi->proxy_context = NULL;

	smartpools_open_context(&ppi->sp_context, &error);
	if (!ppi->sp_context && !error) {
		error = isi_system_error_new(ENOMEM,
		    "Cannot open sp_context");
		goto out;
	}
	UFP_OPEN_PPI_SMARTPOOLS_FAILURE
	ON_ISI_ERROR_GOTO(out, error);

	/*
	 * Utilizing provider_id, get corresponding provider
	 */
	ppi->cp_context = cpool_context_open(&error);
	UFP_OPEN_PPI_CLOUDPOOLS_FAILURE
	ON_ISI_ERROR_GOTO(out, error);

	isi_proxy_context_open(&ppi->proxy_context, &error);
	UFP_OPEN_PPI_PROXY_FAILURE
	ON_ISI_ERROR_GOTO(out, error);

 out:
	if (error != NULL) {
		/* safe to call with an error */
		isi_cfm_close_policy_provider_info(ppi, &error);
	}

	isi_error_handle(error, error_out);
}

void
isi_cfm_close_policy_provider_info(struct isi_cfm_policy_provider *ppi,
    struct isi_error **error_out)
{
	//To prevent resource leaks, cleanup will continue past the first
	//error
	struct isi_error *error = NULL;

	if (ppi == NULL)
		return;

	if (ppi->proxy_context) {
		isi_proxy_context_close(&ppi->proxy_context, &error);
		isi_multi_error_handle(error, error_out);
		error = NULL;
	}

	if (ppi->cp_context) {
		cpool_context_close(ppi->cp_context);
		ppi->cp_context = NULL;
	}

	if (ppi->sp_context) {
		smartpools_close_context(&ppi->sp_context, &error);
		isi_multi_error_handle(error, error_out);
		error = NULL;
	}
}
