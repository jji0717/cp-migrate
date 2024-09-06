#include <isi_ufp/isi_ufp.h>
#include <isi_upgrade_api/isi_upgrade_api.h>

#include "isirep.h"

void
get_version_status(bool use_bw_failpoints, struct version_status *ver_stat)
{
	bool upgrade_started, local_node_upgraded = false;
	uint32_t version, value;
	struct isi_error *error = NULL;

	upgrade_started = uapi_upgrade_started(&error);
	if (error != NULL) {
		log(FATAL, "uapi_upgrade_started(): %s",
		    isi_error_get_message(error));
	}

	if (upgrade_started) {
		local_node_upgraded = uapi_local_node_upgraded(&error);
		if (error != NULL) {
			log(FATAL, "uapi_local_node_upgraded(): %s",
			    isi_error_get_message(error));
		}
	}

	do {
		if (!local_node_upgraded) {
			/* sys_version = get_kern_osrevision(); */
			version = MSG_VERSION;
			break;
		}

		/* sys_version = uapi_upgrading_from_ver(&error); */

		/*
		 * When adding new SIQ features, follow this model:
		 *

		if (ISI_UPGRADE_API_DISK_ENABLED(<newest feature>)) {
			version = <newest version>;
			break;
		}

		if ISI_UPGRADE_API_DISK_ENABLED(<newest-1 feature>)) {
			version = <newest-1 version>;
			break;
		}

		if ISI_UPGRADE_API_DISK_ENABLED(<newest-2 feature>)) {
			version = <newest-2 version>;
			break;
		}

		...

		ASSERT(ISI_UPGRADE_API_DISK_ENABLED(
		    <oldest feature in upgrade window>));
		version = <oldest version in upgrade window>;

		 */
		if (ISI_UPGRADE_API_DISK_ENABLED(HP_WORM)) {
			version = MSG_VERSION_HALFPIPE;
			break;
		}
		
		ASSERT(ISI_UPGRADE_API_DISK_ENABLED(FOREVER));
		version = MSG_VERSION_RIPTIDE;

	} while (false);

	ver_stat->committed_version = version;
	ver_stat->local_node_upgraded = local_node_upgraded;

	/*
	 * Failpoints to modify reported versions and upgrade status.
	 *
	 * Since bandwidth host connections are designed to operate at the
	 * cluster commit version (as opposed to the job source/target
	 * version), two sets of failpoints exist to facilitate single-cluster
	 * testing. This allows co-resident source and target components, which
	 * may have differing (albeit synthesized) job source/target versions,
	 * to correctly interact with the bandwidth host.
	 */
	if (use_bw_failpoints) {
		UFAIL_POINT_CODE(siq_bw_commit_version_delta, {
			value = RETURN_VALUE;
			if (ver_stat->committed_version <
			     (ver_stat->committed_version + value)) {
				ver_stat->committed_version += value;
				log(NOTICE, "Failpoint: %s delta=0x%x",
				    "siq_bw_commit_version_delta", value);
			}
		});

		UFAIL_POINT_CODE(siq_bw_local_node_upgraded, {
			value = RETURN_VALUE;
			if (value != 0) {
				ver_stat->local_node_upgraded = true;
				log(NOTICE, "Failpoint: "
				    "siq_bw_local_node_upgraded");
			}
		});
	} else {
		UFAIL_POINT_CODE(siq_commit_version_delta, {
			value = RETURN_VALUE;
			if (ver_stat->committed_version <
			     (ver_stat->committed_version + value)) {
				ver_stat->committed_version += value;
				log(NOTICE, "Failpoint: %s delta=0x%x",
				    "siq_commit_version_delta", value);
			}
		});

		UFAIL_POINT_CODE(siq_local_node_upgraded, {
			value = RETURN_VALUE;
			if (value != 0) {
				ver_stat->local_node_upgraded = true;
				log(NOTICE, "Failpoint: "
				    "siq_local_node_upgraded");
			}
		});
	}
}

void
log_version_ufp(uint32_t version, const char *component, const char *context)
{
	uint32_t value;

	/* Specify unique failpoint value to facilitate log search. */
	UFAIL_POINT_CODE(siq_log_version, {
		value = RETURN_VALUE;
		log(NOTICE, "Failpoint: siq_log_version instance=%d "
		    "default=0x%x version=0x%x component=%s context=%s", value,
		    MSG_VERSION, version, component, context);
	});
}

void
log_job_version_ufp(struct siq_job_version *job_version, const char *component,
   const char *context)
{
	uint32_t value;

	/* Specify unique failpoint value to facilitate log search. */
	UFAIL_POINT_CODE(siq_log_job_version, {
		value = RETURN_VALUE;
		log(NOTICE, "Failpoint: siq_log_job_version instance=%d "
		    "default=0x%x common=0x%x local=0x%x component=%s "
		    "context=%s", value, MSG_VERSION, job_version->common,
		    job_version->local, component, context);
	});
}
