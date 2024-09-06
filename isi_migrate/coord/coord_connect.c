#include <netdb.h>
#include <isi_ufp/isi_ufp.h>
#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/migr/siq_workers_utils.h"
#include "isi_cpool_cbm/isi_cbm_error.h"
#include "isi_cpool_cbm/isi_cbm_file_versions.h"
#include "coord.h"

#define BUFFER_SIZE 4096
#define INITIAL_CONNECTION_TRIES 5
#define MAX_TARGET_NODE_CONNECT_TRIES	10

static struct timeval tmonitor_timeout = {30, 0};

/*  ____       _            _         _   _      _                     
 * |  _ \ _ __(_)_   ____ _| |_ ___  | | | | ___| |_ __   ___ _ __ ___ 
 * | |_) | '__| \ \ / / _` | __/ _ \ | |_| |/ _ \ | '_ \ / _ \ '__/ __|
 * |  __/| |  | |\ V / (_| | ||  __/ |  _  |  __/ | |_) |  __/ |  \__ \
 * |_|   |_|  |_| \_/ \__,_|\__\___| |_| |_|\___|_| .__/ \___|_|  |___/
 *                                                |_|                  
 */

/*
 * Connects to target monitor and stores socket in tmonitor context.
 */
static void
tmonitor_connect(struct tmonitor_ctx *tctx, uint32_t max_ver,
    uint32_t *prot_ver, struct isi_error **error_out)
{
	struct isi_error *error = NULL, *connect_error = NULL;

	ASSERT(tctx);
	ASSERT(tctx->sock == -1);

	log(TRACE, "%s: connecting to target monitor on %s:%d",
	    __func__, tctx->target_name, tctx->sworker_port);

	/* Make a request to sworker for its cluster name, devids, and ips. */
	tctx->sock = connect_to(tctx->target_name, tctx->sworker_port,
	    POL_DENY_FALLBACK, max_ver, prot_ver, tctx->forced_addr,
	    false, &connect_error);
	if (tctx->sock < 0) {
		error = isi_siq_error_new(E_SIQ_GEN_NET,
		    "%s: failed connect to sworker on %s:%d: %s", __func__,
		    tctx->target_name, tctx->sworker_port,
		    isi_error_get_message(connect_error));
		isi_error_free(connect_error);
		goto out;
	}

out:
	isi_error_handle(error, error_out);
}

static void
target_resp_msg_unpack(struct generic_msg *m, uint32_t prot_ver,
    struct target_resp_msg *ret, struct isi_error **error_out)
{
	struct old_target_resp_msg2 *tresp_msg = &m->body.old_target_resp2;
	struct siq_ript_msg *ript = &m->body.ript;
	struct isi_error *error = NULL;

	if (prot_ver < MSG_VERSION_RIPTIDE) {
		if (m->head.type != OLD_TARGET_RESP_MSG2) {
			log(FATAL,
			    "%s: Received unexpected message of type %s",
			    __func__, msg_type_str(m->head.type));
		}

		ret->error = tresp_msg->error;
		ret->target_cluster_name = tresp_msg->target_cluster_name;
		ret->target_cluster_id = tresp_msg->target_cluster_id;
		ret->ipslen = tresp_msg->ipslen;
		ret->ips = (uint8_t *)tresp_msg->ips;
		ret->domain_id = tresp_msg->domain_id;
		ret->domain_generation = tresp_msg->domain_generation;
	} else {
		if (m->head.type != build_ript_msg_type(TARGET_RESP_MSG)) {
			log(FATAL,
			    "%s: Received unexpected message of type %s",
			    __func__, msg_type_str(m->head.type));
		}

		ript_msg_get_field_str(ript, RMF_ERROR, 1, &ret->error,
		    &error);
		if (error != NULL) {
			if (!isi_system_error_is_a(error, ENOENT))
				goto out;
			isi_error_free(error);
			error = NULL;
		} else  {
			// The message contains an error so propagate it up.
			goto out;
		}

		RIPT_MSG_GET_FIELD(str, ript, RMF_TARGET_CLUSTER_NAME, 1,
		    &ret->target_cluster_name, &error, out);
		RIPT_MSG_GET_FIELD(str, ript, RMF_TARGET_CLUSTER_ID, 1,
		    &ret->target_cluster_id, &error, out);
		RIPT_MSG_GET_FIELD_ENOENT(uint64, ript, RMF_DOMAIN_ID, 1,
		    &ret->domain_id, &error, out);
		RIPT_MSG_GET_FIELD_ENOENT(uint32, ript, RMF_DOMAIN_GENERATION,
		    1, &ret->domain_generation, &error, out);

		ript_msg_get_field_bytestream(ript, RMF_IPS, 1,
		    &ret->ips, &ret->ipslen, &error);
		if (error != NULL)
			goto out;

		ript_msg_get_field_bytestream(ript, RMF_CBM_FILE_VERSIONS,
		    1, &ret->tgt_cbm_vers_buf, &ret->tgt_cbm_vers_buf_len,
		    &error);
		if (error != NULL)
			goto out;

		RIPT_MSG_GET_FIELD_ENOENT(uint8, ript, RMF_COMPLIANCE_DOM_VER, 1,
		    &ret->compliance_dom_ver, &error, out);
		RIPT_MSG_GET_FIELD_ENOENT(uint8, ript,
		    RMF_TARGET_WORK_LOCKING_PATCH, 1,
		    &ret->target_work_locking_patch, &error, out);
		RIPT_MSG_GET_FIELD_ENOENT(uint8, ript,
		    RMF_COMP_LATE_STAGE_UNLINK_PATCH, 1,
		    &ret->comp_late_stage_unlink_patch, &error, out);
	}

out:
	isi_error_handle(error, error_out);
}

/*
 * Initialize tmonitor with policy details, or request only IPs.
 */
static int
tmonitor_initialize(struct tmonitor_ctx *tctx, char ***ip_list,
    enum target_init_option option, uint32_t prot_ver,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct generic_msg GEN_MSG_INIT_CLEAN(cm);
	struct old_target_init_msg *tinit_msg = &cm.body.old_target_init;
	struct target_resp_msg tresp = {};
	struct siq_ript_msg *ript = NULL;
	struct error_msg *err_msg = &cm.body.error;
	int i;
	int count = -1;
	struct hostent *he;
	char node[MAXHOSTNAMELEN];
	char local_cluster_name[MAXHOSTNAMELEN];
	char *addr_str = NULL;
	bool identical_cluster_names, use_sent_ip;
	bool remote_host_ip_in_sent_ips = false;
	struct lnn_ipx_pair *pairs = NULL;
	char *msg_buf = NULL;
	struct in_addr addr4;
	struct in6_addr addr6;
	int siqerr = -1;
	int ret = -1;
	struct isi_cbm_file_versions *src_cbm_vers = NULL;
	struct isi_cbm_file_versions *tgt_cbm_vers = NULL;
	int src_compliance_dom_ver = 0;

	ASSERT(tctx->sock != -1);

	gethostname(local_cluster_name, MAXHOSTNAMELEN);
	cluster_name_from_node_name(local_cluster_name);

	src_compliance_dom_ver =
	    get_compliance_dom_ver(tctx->src_root_dir, &error);
	if (error)
		goto out;

	/*
	 * Bug 169472: compliance v1 -> v2 patch.
	 * Either this needs to be a patched Riptide+ cluster or this is a
	 * pre-Riptide cluster which does not support SyncIQ NDU.
	 */
	if (src_compliance_dom_ver == 1) {
		if (access(SIQ_COMP_V1_V2_PATCH_FILE, F_OK) == 0) {
			tctx->flags |= FLAG_COMP_V1_V2_PATCH;
			log(DEBUG, "%s: Detected compliance v1->v2 patch",
			    __func__);
		} else if (errno != ENOENT) {
			error = isi_system_error_new(errno, "Failed to check "
			    "for support of compliance v1->v2.");
			log(ERROR, "%s: %s", __func__,
			    isi_error_get_message(error));
			goto out;
		}
	}

	if (prot_ver < MSG_VERSION_RIPTIDE) {
		cm.head.type = OLD_TARGET_INIT_MSG;
		tinit_msg->restricted_target_name =
		    tctx->target_restrict ? tctx->target_name : NULL;
		tinit_msg->policy_id = tctx->policy_id;
		tinit_msg->policy_name = tctx->policy_name;
		tinit_msg->run_id = tctx->run_id;
		tinit_msg->target_dir = tctx->target_dir;
		tinit_msg->src_root_dir = tctx->src_root_dir;
		tinit_msg->src_cluster_name = local_cluster_name;
		tinit_msg->src_cluster_id = get_cluster_guid();
		tinit_msg->last_target_cluster_id =
		    tctx->last_target_cluster_id;
		tinit_msg->option = option;
		tinit_msg->loglevel = tctx->loglevel;
		tinit_msg->flags = tctx->flags;
		if (option != OPTION_INIT)
			tinit_msg->flags &= (~FLAG_STF_LINMAP_DELETE);
	} else {
		cm.head.type = build_ript_msg_type(TARGET_INIT_MSG);
		ript = &cm.body.ript;
		ript_msg_init(ript);
		ript_msg_set_field_str(ript, RMF_RESTRICTED_TARGET_NAME, 1,
		    tctx->target_restrict ? tctx->target_name : NULL);
		ript_msg_set_field_str(ript, RMF_POLICY_ID, 1,
		    tctx->policy_id);
		ript_msg_set_field_str(ript, RMF_POLICY_NAME, 1,
		    tctx->policy_name);
		ript_msg_set_field_uint64(ript, RMF_RUN_ID, 1,
		    tctx->run_id);
		ript_msg_set_field_str(ript, RMF_TARGET_DIR, 1,
		    tctx->target_dir);
		ript_msg_set_field_str(ript, RMF_SRC_ROOT_DIR, 1,
		    tctx->src_root_dir);
		ript_msg_set_field_str(ript, RMF_SRC_CLUSTER_NAME, 1,
		    local_cluster_name);
		ript_msg_set_field_str(ript, RMF_SRC_CLUSTER_ID, 1,
		    get_cluster_guid());
		ript_msg_set_field_str(ript, RMF_LAST_TARGET_CLUSTER_ID, 1,
		    tctx->last_target_cluster_id);
		ript_msg_set_field_uint32(ript, RMF_OPTION, 1, option);
		ript_msg_set_field_uint32(ript, RMF_LOGLEVEL, 1,
		    tctx->loglevel);
		ript_msg_set_field_uint32(ript, RMF_FLAGS, 1,
		    (option == OPTION_INIT) ? tctx->flags :
		    (tctx->flags & (~FLAG_STF_LINMAP_DELETE)));
		ript_msg_set_field_uint64(ript, RMF_WI_LOCK_MOD, 1,
		    g_ctx.rc.target_wi_lock_mod);

		if (access(SIQ_COMP_LATE_STAGE_UNLINK_PATCH_FILE, F_OK) == 0) {
			ript_msg_set_field_uint8(ript,
			    RMF_COMP_LATE_STAGE_UNLINK_PATCH, 1, 1);
		} else if (errno == ENOENT) {
			error = isi_siq_error_new(E_SIQ_VER_UNSUPPORTED,
			    "Could not access Compliance mode late stage"
			    " unlink patch file. Has the source been "
			    "upgraded and committed with patch?");
			fatal_error_from_isi_error(false, error);
		} else {
			error = isi_system_error_new(errno,
			    "Failed to check support for compliance late stage"
			    " unlink.");
			goto out;
		}
	}

	if (msg_send(tctx->sock, &cm)) {
		error = isi_siq_error_new(E_SIQ_GEN_NET,
		    "%s: can't send cluster "
		    "message to sworker: %s", __func__, strerror(errno));
		goto out;
	}

	free_generic_msg(&cm);

	/*
	 * Reset all the supported versions and let target message decide
	 * the common version.
	 */
	g_ctx.rc.flags &= ~(FLAG_VER_3_5);
	g_ctx.rc.flags &= ~(FLAG_VER_CLOUDPOOLS);

	/*
	 * Options for the target sworker:
	 *
	 *    - OPTION_INIT: Start up the tmonitor, and sworker replies with a
	 *      list of target IPs. Leave open the connection to the tmonitor.
	 *
         *    - OPTION_RESTART: Same as OPTION_INIT, except the sworker does
	 *      not clear target database state.  Used during job restarts.
	 */

again:
	ret = read_msg_from_fd(tctx->sock, &cm, true, false, &msg_buf);
	if (ret != READ_MSG_RESP_OK) {
			error = isi_siq_error_new(E_SIQ_GEN_NET,
			    "%s: Can't receive "
			    "message from sworker: %s", __func__, 
			    strerror(errno));
			goto out;
	}

	if (cm.head.type == ERROR_MSG) {
		error = isi_siq_error_new(E_SIQ_GEN_NET,
		    "Error from remote sworker %s: %s",
		    tctx->target_name, err_msg->str);
		goto out;
	}

	if (cm.head.type == COMMON_VER_MSG) {
		/*
		 * Use common version by trying each version from the
		 * highest to lowest. First reset all the versions 
		 * that source supports and then find the common version.
		 */
		g_ctx.rc.flags |= (cm.body.common_ver.version & FLAG_VER_3_5);
		g_ctx.rc.flags |=
		    (cm.body.common_ver.version & FLAG_VER_CLOUDPOOLS);
		g_ctx.curr_ver = (cm.body.common_ver.version & FLAG_VER_3_5);
		g_ctx.curr_ver |=
		    (cm.body.common_ver.version & FLAG_VER_CLOUDPOOLS);

		/* Starting with Riptide, use job_version, not curr_ver. */
		ASSERT((g_ctx.curr_ver &
		    ~(FLAG_VER_3_5 | FLAG_VER_CLOUDPOOLS)) == 0);

		/*
		 * We know that next message is going to be target_resp_msg or
		 * general_signal_msg. Hence go back and read the next message.
		 */
		free(msg_buf);
		msg_buf = NULL;
		goto again;
	}

	if (cm.head.type == GENERAL_SIGNAL_MSG) {
		ASSERT(cm.body.general_signal.signal == SIQ_COMP_V1_V2_PATCH,
		    "signal == %d", cm.body.general_signal.signal);
		g_ctx.rc.flags |= FLAG_COMP_V1_V2_PATCH;

		/*
		 * Next message will be target_resp_msg so read another message.
		 */
		free(msg_buf);
		msg_buf = NULL;
		goto again;
	}

	if (!(g_ctx.curr_ver & FLAG_VER_CLOUDPOOLS) &&
	    (g_ctx.rc.flags & FLAG_FILE_SPLIT)) {
		log(INFO, "Disabling file splitting: Target cluster is "
		    "running a version earlier than OneFS 7.1.1");
		g_ctx.rc.flags &= ~(FLAG_FILE_SPLIT);
	}

	target_resp_msg_unpack(&cm, prot_ver, &tresp, &error);
	if (error != NULL)
		goto out;

	if (tresp.error) {
		siq_source_set_unrunnable(g_ctx.record);

		/* hack to fix bug 75403. the tmonitor can't send an
		 * siq_error since existing coordinators won't expect
		 * or handle them. to convey the actual error type, we
		 * abuse the target_resp_msg by stuffing the value into
		 * the domain generation field so capable coords can
		 * generate a more specific error and alert. */
		siqerr = (tresp.domain_generation != 0) ?
		    tresp.domain_generation : E_SIQ_GEN_NET;

		error = isi_siq_error_new(siqerr, "%s (unrunnable)",
		    tresp.error);

		fatal_error_from_isi_error(false, error);
	}

	/*
	 * Halfpipe: SyncIQ does not support resync-prep from a Compliance
	 * v1 to a Compliance v2 directory
	 *
	 * The combination where the target dir is a Compliance v2 and the
	 * source dir is a 0(no domain) is invalid and would have failed the
	 * regular sync much earlier.
	 */
	if ((JOB_SPEC->common->action == SIQ_ACT_FAILBACK_PREP) &&
	    (tresp.compliance_dom_ver == 2 && src_compliance_dom_ver == 1)) {
		error = isi_siq_error_new(E_SIQ_VER_UNSUPPORTED,
		    "ERROR: SyncIQ does not support resync prep from a "
		    "Compliance v1 to a Compliance v2 directory");
		fatal_error_from_isi_error(false, error);
	}

	/*
	 * Halfpipe: SyncIQ does not support Compliance v2 to v1 domain syncs
	 */
	if (src_compliance_dom_ver == 2 && tresp.compliance_dom_ver < 2) {
		error = isi_siq_error_new(E_SIQ_VER_UNSUPPORTED,
		    "ERROR: Destination must be a Compliance v2 directory");
		fatal_error_from_isi_error(false, error);
	}

	/* Ensure that source and target both are compatible from compliance mode
	 * late stage directry unlink patch perspective
	 */
	if (src_compliance_dom_ver > 0) { // source has compliance domain
		if (access(SIQ_COMP_LATE_STAGE_UNLINK_PATCH_FILE, F_OK) == 0) {
			/* Source has compliance mode late stage unlink patch */
			log(DEBUG, "%s: source has late stage unlink patch",
			    __func__);
			if (tresp.comp_late_stage_unlink_patch != 1) {
				error = isi_siq_error_new(E_SIQ_VER_UNSUPPORTED,
				    "Source supports compliance mode late stage"
				    " unlink while target does not. Please "
				    "upgrade target cluster.");
				fatal_error_from_isi_error(false, error);
			}
		} else if (errno == ENOENT) {
			error = isi_siq_error_new(E_SIQ_VER_UNSUPPORTED,
			    "Could not access Compliance mode late stage"
			    " unlink patch file. Has the source been "
			    "upgraded and committed with patch?");
			fatal_error_from_isi_error(false, error);
		} else {
			error = isi_system_error_new(errno,
			    "Failed to check for support of compliance mode late"
			    " stage dir unlink patch.");
			goto out;
		}
	}

	if (tresp.tgt_cbm_vers_buf != NULL && tresp.tgt_cbm_vers_buf_len > 0) {
		// Cloudpools file versions
		src_cbm_vers = isi_cbm_file_versions_get_supported();
		if (src_cbm_vers == NULL) {
			error = isi_siq_error_new(E_SIQ_CLOUDPOOLS,
			    "%s: Failed to create Cloudpools file "
			    "versions: %s", __func__, strerror(errno));
			goto out;
		}

		tgt_cbm_vers = isi_cbm_file_versions_deserialize(
		    tresp.tgt_cbm_vers_buf, tresp.tgt_cbm_vers_buf_len,
		    &error);
		if (error != NULL)
			goto out;

		g_ctx.common_cbm_file_vers = isi_cbm_file_versions_common(
		    src_cbm_vers, tgt_cbm_vers);

		isi_cbm_file_versions_destroy(src_cbm_vers);
		src_cbm_vers = NULL;
		isi_cbm_file_versions_destroy(tgt_cbm_vers);
		tgt_cbm_vers = NULL;
	} else {
		g_ctx.common_cbm_file_vers = isi_cbm_file_versions_create();
		if (g_ctx.common_cbm_file_vers == NULL) {
			error = isi_siq_error_new(E_SIQ_CLOUDPOOLS,
			    "%s: Failed to create Cloudpools file "
			    "versions: %s", __func__, strerror(errno));
			goto out;
	       }
	}

	copy_or_upgrade_inx_addr_list((char *)tresp.ips,
	    (unsigned)tresp.ipslen, g_ctx.curr_ver, &pairs, &count);
	ASSERT(count > 0);

	/* BEJ: Bug 137879: dont set if doing domain_mark */
	if (!(g_ctx.rc.flags & FLAG_DOMAIN_MARK)) {
		siq_source_set_target_guid(g_ctx.record,
		    tresp.target_cluster_id);
		siq_source_record_save(g_ctx.record, &error);
		if (error)
			fatal_error(false, SIQ_ALERT_POLICY_FAIL,
			    "Error saving target cluster id: %s",
			    isi_error_get_message(error));
	}
	g_ctx.rc.domain_id = tresp.domain_id;
	g_ctx.rc.domain_generation = tresp.domain_generation;

	log(TRACE, "%s: target: name=%s  number of nodes=%d",
	    __func__, tresp.target_cluster_name, count);

	if (count == 0) {
		error = isi_siq_error_new(E_SIQ_GEN_NET,
		    "Remote '%s' has no IPs", tctx->target_name);
		goto out;
	}

	*ip_list = allocate_ip_list(count);

	/* Look through results and get IPs from DNS or remote cluster.
	 * DNS is used because with NAT the IPs sent from the remote cluster
	 * may not be routable from the local cluster.
	 */

	use_sent_ip = true;

	/* If source and target clusters have the same name, DNS lookup will
	 * be wrong, so use sent IP. */
	identical_cluster_names = !strcmp(tresp.target_cluster_name,
	    local_cluster_name);

	/* If the target_name is an IP (as opposed to a hostname), and that IP
	 * is in the list of IPs sent back, then we can guarantee the sent IPs
	 * are routable between the local and remote clusters. */
	if (inet_pton(AF_INET, tctx->target_name, &addr4) == 1) {
		for (i = 0; i < count; i++) {
			if (pairs[i].addr.inx_family == AF_INET &&
			    pairs[i].addr.inx_addr4 == addr4.s_addr) {
				log(DEBUG, "Found remote host IP in list of "
				    "sent IPs; skipping DNS resolution.");
				remote_host_ip_in_sent_ips = true;
				break;
			}
		}
	} else if (inet_pton(AF_INET6, tctx->target_name, &addr6) == 1) {
		for (i = 0; i < count; i++) {
			if (pairs[i].addr.inx_family == AF_INET6 &&
			    pairs[i].addr.inx_addr6 == addr6.s6_addr) {
				log(DEBUG, "Found remote host (v6) IP in list "
				    "of sent IPs; skipping DNS resolution.");
				remote_host_ip_in_sent_ips = true;
				break;
			}
		}
	}

	for (i = 0; i < count; i++) {
		if (!identical_cluster_names && !remote_host_ip_in_sent_ips && 
		    !g_ctx.rc.skip_lookup) {
			sprintf(node, "%s-%d", tresp.target_cluster_name,
			    pairs[i].lnn);
			if ((he = gethostbyname(node))) {
				strcpy((*ip_list)[i], ip2text(he->h_addr));
				use_sent_ip = false;
			} else
				use_sent_ip = true;
		} else if (g_ctx.rc.skip_lookup) {
			log(TRACE, "Skipped gethostbyname due to policy setting");
		}
		if (use_sent_ip) {
			if ((addr_str = inx_addr_to_str(&pairs[i].addr))
			    == NULL) {
				error = isi_siq_error_new(E_SIQ_GEN_NET,
				    "Error converting address to human "
				    "readable format");
				goto out;
			}
			strncpy((*ip_list)[i], addr_str, INET6_ADDRSTRLEN);
		}
		log(TRACE, "%s: lnn=%d %s", __func__, pairs[i].lnn,
		    (*ip_list)[i]);
	}

	if (access(SIQ_TARGET_WORK_LOCKING_PATCH_FILE, F_OK) == 0) {
		g_ctx.rc.target_work_locking_patch =
		    tresp.target_work_locking_patch;
	} else if (errno == ENOENT)
		g_ctx.rc.target_work_locking_patch = false;
	else {
		error = isi_system_error_new(errno,
		    "Failed to check for support of target "
		    "work locking patch.");
		goto out;
	}

out:
	if (src_cbm_vers != NULL)
		isi_cbm_file_versions_destroy(src_cbm_vers);

	if (pairs) {
		free(pairs);
		pairs = NULL;
	}

	free(msg_buf);

	isi_error_handle(error, error_out);

	return count;
}

static void
tmonitor_disconnect(struct tmonitor_ctx *tctx)
{
	if (tctx->sock != -1) {
		migr_rm_fd(tctx->sock);
		close(tctx->sock);
		tctx->sock = -1;
	}
}

/*   ____      _ _ _                _        
 *  / ___|__ _| | | |__   __ _  ___| | _____ 
 * | |   / _` | | | '_ \ / _` |/ __| |/ / __|
 * | |__| (_| | | | |_) | (_| | (__|   <\__ \
 *  \____\__,_|_|_|_.__/ \__,_|\___|_|\_\___/
 */

static int
target_cancel_callback(struct generic_msg *m, void *ctx)
{
	struct generic_msg ack = {};

	log(NOTICE, "Received target cancel for policy %s", g_ctx.rc.job_name);

	ack.head.type = GENERIC_ACK_MSG;
	ack.body.generic_ack.id = g_ctx.rc.job_id;
	ack.body.generic_ack.code = ACK_OK;
	msg_send(g_ctx.tctx.sock, &ack);

	if (siq_job_cancel(g_ctx.rc.job_id) != 0)
		log(ERROR, "Failed to signal cancel of policy %s",
		    g_ctx.rc.job_name);
	return 0;
}

static int
tmonitor_disconnect_callback(struct generic_msg *m, void *ctx)
{
	struct isi_error *error = NULL;
	struct coord_ctx *coord_ctx = (struct coord_ctx *)ctx;

	log(INFO, "Unexpected disconnect from tmonitor. "
	    "Reconnecting to target.");

	disconnect_from_tmonitor();
	connect_to_tmonitor(&error);
	if (error) {
		fatal_error(false, SIQ_ALERT_TARGET_CONNECTIVITY,
		    isi_error_get_message(error));
	}

	if (coord_ctx->waiting_for_tgt_msg) {
		/*
		 * stop the migr_process() loop so that we
		 * again send the message to target
		 */
		migr_force_process_stop();
	}

	/* Dynamically reallocate workers in case of target group change */
	if (coord_ctx->workers_configured)
		configure_workers(false);

	log(INFO, "Successful reconnection to tmonitor");
	return 0;
}

static int
tmonitor_shutdown_callback(struct generic_msg *m, void *ctx)
{
	fatal_error(false, SIQ_ALERT_TARGET_CONNECTIVITY,
	    "Policy can not run because the target monitor has shut down: %s",
	    m->body.target_monitor_shutdown.msg);
	return 0;
}

static int
tmonitor_update_timer_callback(void *ctx)
{
	send_status_update_to_tmonitor();
	migr_register_timeout(&tmonitor_timeout, 
	    tmonitor_update_timer_callback, 0);
	return 0;
}

/*  ____        _     _ _        _   _      _                     
 * |  _ \ _   _| |__ | (_) ___  | | | | ___| |_ __   ___ _ __ ___ 
 * | |_) | | | | '_ \| | |/ __| | |_| |/ _ \ | '_ \ / _ \ '__/ __|
 * |  __/| |_| | |_) | | | (__  |  _  |  __/ | |_) |  __/ |  \__ \
 * |_|    \__,_|_.__/|_|_|\___| |_| |_|\___|_| .__/ \___|_|  |___/
 *                                           |_|                  
 */

/*
 * Connect to a remote cluster, retrieve a list of IPs, disconnect.
 */
int
get_cluster_ips(char *cluster_name, int sworker_port, uint32_t max_version, /////获得整个cluster中所有node的ip
    char ***ip_list, struct isi_error **error_out)
{
	int n_nodes = -1;
	uint32_t ver;
	struct isi_error *error = NULL;
	struct tmonitor_ctx tctx = {
		.sock = -1,
		.target_name = cluster_name,
		.sworker_port = sworker_port,
	};

	log(INFO, "%s called", __func__); ////TRACE -> INFO

	/* If cluster is local we can get IPs without contacting tmonitor */
	if (strcmp(cluster_name, "localhost") == 0 ||
	    strcmp(cluster_name, "127.0.0.1") == 0 ||
	    strcmp(cluster_name, "::1") == 0) {
		n_nodes = get_local_cluster_info(NULL, ip_list, NULL, NULL, 0,
		    NULL, true, true, NULL, &error);
		goto out;
	}

	tmonitor_connect(&tctx, max_version, &ver, &error);
	if (error)
		goto out;

	tmonitor_initialize(&tctx, ip_list, OPTION_IPS_ONLY, ver, &error);
	if (error)
		goto out;

	tmonitor_disconnect(&tctx);

out:
	isi_error_handle(error, error_out);
	return n_nodes;
}

/*
 * Query the remote cluster for the set of sworker ip's. This also initiates
 * the target monitor on the node connected to.
 */
void
connect_to_tmonitor(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool initial_connection;
	struct fmt FMT_INIT_CLEAN(target_fmt); /* To log list of target IPs */
	struct in_addr addr4;
	struct in6_addr addr6;
	int i, secs_between_tries = 2, tries, n_target_nodes = -1;
	int old_ip_index = 0;
	struct tmonitor_ctx *tctx = &g_ctx.tctx;
	char **ip_list = NULL;
	uint32_t prot_ver = 0;
	bool assigned_job_ver = false, is_new_job_ver = false;
	char *context;
	uint32_t max_ver, value;
	struct siq_job_version job_ver, prev_job_ver;

	ASSERT(tctx->sock == -1);

	if (g_ctx.new_tgt_nodes) {
		/* this can happen when connect_to_tmonitor is called and
		 * migr_process is started before configure_workers is called,
		 * and the tmonitor connection is interrupted. temporary
		 * migr_process loops aren't desirable, but they are used
		 * anyway (see pre_sync_snap) */
		free(g_ctx.new_tgt_nodes);
		g_ctx.new_tgt_nodes = NULL;
	}

	tctx->target_name = g_ctx.rc.target;
	tctx->sworker_port = global_config->root->coordinator->ports->sworker;
	tctx->target_restrict = JOB_SPEC->target->target_restrict;
	tctx->forced_addr = g_ctx.forced_addr;
	tctx->policy_id = g_ctx.rc.job_id;
	tctx->policy_name = g_ctx.rc.job_name;
	tctx->run_id = g_ctx.run_id;
	tctx->src_root_dir = g_ctx.rc.path;
	tctx->target_dir =  g_ctx.rc.targetpath;
	tctx->loglevel = g_ctx.rc.loglevel;
	tctx->flags = g_ctx.rc.flags;

	siq_source_get_target_guid(g_ctx.record, tctx->last_target_cluster_id);

	initial_connection = (g_ctx.tgt_nodes == NULL);

	if (initial_connection) {
		/* First time we've gathered IPs of the target */
		tries = INITIAL_CONNECTION_TRIES;
	} else {
		/* Otherwise, there was a connection failure to the target's
		 * tmonitor, and a reconnect has to be made.
		 * For big clusters, we don't want to try too many times to
		 * reconnect to the target.*/
		tries = MAX_TARGET_NODE_CONNECT_TRIES;
	}

	get_composite_job_ver(g_ctx.record, g_ctx.use_restore_job_vers,
	    &job_ver);
	get_prev_composite_job_ver(g_ctx.record, g_ctx.use_restore_job_vers,
	    &prev_job_ver);

	if (job_ver.common == 0) {
		ASSERT(job_ver.local == 0);
		max_ver = g_ctx.ver_stat.committed_version;
	} else {
		ASSERT(job_ver.local != 0);
		max_ver = job_ver.common;
	}

	/* A failpoint to facilitate synthesized down-level testing in
	 * single-cluster scenarios; this causes both source & target to
	 * think the other is down-level. */
	UFAIL_POINT_CODE(siq_initial_handshake_version, {
		value = RETURN_VALUE;
		if (value >= prev_job_ver.common &&
		    value >= MSG_VERSION_CHOPUVILLE &&
		    value < max_ver) {
			log(NOTICE, "Failpoint: replace "
			    "handshake max_ver=0x%x with "
			    "max_ver=0x%x", max_ver, value);
			max_ver = value;
		}
	});

	/* 
	 * Try mulitiple times to get a connection to the target cluster,
	 * leaving open a file descriptor to a target_monitor on a target
	 * node. If the policy has just started up, and no IPs have previously
	 * been obtained from the target, then the only thing that we can use
	 * to connect to the target is the target name or IP address in the
	 * policy definition.
	 *
	 * In the case that a previous connection to the policy's
	 * target_monitor has been lost, a new connection can be made with
	 * preferably the target name or IP in the policy definition. If for
	 * some reason that fails, there still is the list of IPs of the
	 * target cluster that was gathered when the connection was last
	 * initiated. That IPs in that list can be used as an alternative
	 * means of contacting the target cluster.
	 *
	 * Try connecting to the target a fixed number of times before giving
	 * up, with an exponential backoff.
	 */

	for (i = 0; i < tries; i++) {
		tmonitor_connect(&g_ctx.tctx, max_ver, &prot_ver, &error);
		if (error) {
			/*
			 * Bug 166691
			 * don't log msg_handshake socket errors
			 */
			if (prot_ver != SHAKE_FAIL)
				log(ERROR, "tmonitor_connect failure: %s",
				    isi_error_get_message(error));
			isi_error_free(error);
			error = NULL;
			continue;
		}

		/* Assign & save the job version after the first successful
		 * connect but before the initialize. This ensures a
		 * consistent version experience for tmonitor regardless of
		 * subsequent events, e.g. job retries. */
		if (!assigned_job_ver) {
			if (g_ctx.failover || g_ctx.failover_revert) {
				context = "restore";
				if (prot_ver != job_ver.common) {
					log(FATAL, "Job version mismatch: "
					    "expected=0x%x, actual=0x%x",
					    job_ver.common, prot_ver);
				}

			} else if (job_ver.common == 0) {
				/* The previous job (if any) succeeded;
				 * choose a new ver. */
				context = "new";
				job_ver.local =
				    g_ctx.ver_stat.committed_version;
				job_ver.common = prot_ver;
				set_composite_job_ver(g_ctx.record,
				    g_ctx.use_restore_job_vers, &job_ver);

				/* If this is a non-restore job then set the
				 * restore vers to match the non-restore vers
				 * so any future restore job will operate at
				 * a matching versions. */
				if (!g_ctx.use_restore_job_vers) {
					set_composite_job_ver(
					    g_ctx.record, true, &job_ver);
					set_prev_composite_job_ver(
					    g_ctx.record, true, &prev_job_ver);
				}
				siq_source_record_save(g_ctx.record, &error);
				if (error != NULL) {
					goto out;
				}
				is_new_job_ver = true;
			} else {
				/* The previous job did not succeed;
				 * use the existing ver. */
				context = "existing";
				if (prot_ver != job_ver.common) {
					log(FATAL, "Job version mismatch: "
					    "expected=0x%x, actual=0x%x",
					    job_ver.common, prot_ver);
				}
			}
			memcpy(&g_ctx.job_ver, &job_ver, sizeof(job_ver));
			g_ctx.local_job_ver_update =
			    (prev_job_ver.local < g_ctx.job_ver.local &&
			    prev_job_ver.local > 0);
			g_ctx.prev_local_job_ver = prev_job_ver.local;

			ilog(IL_INFO, "%s called, Using %s job version: local=0x%x, "
			    "common=0x%x", __func__,context, job_ver.local,
			    job_ver.common);
			log_job_version_ufp(&job_ver, SIQ_COORDINATOR, context);

			assigned_job_ver = true;
		}

		n_target_nodes = tmonitor_initialize(&g_ctx.tctx, &ip_list,
		    initial_connection ? OPTION_INIT : OPTION_RESTART,
		    prot_ver, &error);

		/* A list of IPs was returned. No need to try again */
		if (n_target_nodes > 0)
			break;

		tmonitor_disconnect(&g_ctx.tctx);

		/* Can't proceed if a connection was made, but there are no
		 * IPs to use on target */
		if (n_target_nodes == 0)
			goto out;

		/* If < 0, try again
		 *
		 * Else, if tmonitor_initialize failed with a SIQ error such that
		 * the target dir doesn't exist (happens only when we are trying
		 * to sync from a compliance v2 to another complaince v2 domain)
		 * then we do not retry and just fail directly.
		 * */
		if (error) {
			if (isi_error_is_a(error, ISI_SIQ_ERROR_CLASS) &&
			    (isi_siq_error_get_siqerr(
			    (struct isi_siq_error *)error) ==
			    E_SIQ_COMP_TGTDIR_ENOENT)) {
				goto out;
			}
		}

		log(INFO, "%s. %s", isi_error_get_message(error),
		    i != tries - 1 ? "Will try again." : "Giving up.");

		/* Free things that may have been set in a failed call to
		 * tmonitor_initialize */
		if (ip_list) {
			free_ip_list(ip_list);
			ip_list = NULL;
		}
		isi_error_free(error);
		error = NULL;

		/* Sleep between failed connection attempts */
		if (i != tries - 1) {
			siq_nanosleep(secs_between_tries, 0);

			/* Exponential backoff of connection attempts */
			secs_between_tries *= 2;
		}

		/* If this is a reconnect attempt, try a different IP (from
		 * the old IP list) for each try. Use a modulo of the number
		 * of IPs in the old list to ensure that we rotate through the
		 * list if the number of old IPs is smaller than the number of
		 * times we're trying to connect to the target. */
		if (!initial_connection) {
			tctx->target_name =
			    g_ctx.tgt_nodes->nodes[old_ip_index++ %
			    g_ctx.tgt_nodes->count]->host;
		}
	}

	/* After all the tries, still didn't get a connection */
	if (n_target_nodes == -1) {
		error = isi_siq_error_new(E_SIQ_GEN_NET, "Error connecting to "
		    "target cluster %s:%d. Job must exit", g_ctx.rc.target,
		    global_config->root->coordinator->ports->sworker);
		goto out;
	}

	if (initial_connection) {
		/* Just a message to indicate that a connection was made after
		 * an initial failure. */
		if (i > 0)
			log(COPY, "Connected to target cluster");
		log(NOTICE, "Started %s / %s / %llu",
		    g_ctx.rc.job_name, g_ctx.rc.job_id, g_ctx.run_id);
	} else {
		log(NOTICE, "Reconnected to target cluster %s using %s",
		    g_ctx.rc.target, tctx->target_name);
	}

	ASSERT(tctx->sock > 0);

	/* A failpoint to facilitate existing-job-ver path testing.
	 * At this point, the tmonitor will have successfully initialized
	 * and saved it's job version. */
	if (is_new_job_ver) {
		UFAIL_POINT_CODE(siq_exit_after_job_version_set, {
			value = RETURN_VALUE;
			if (value == 0) {
				/* Runs atexit-registered routines &
				 * prevents a job restart. */
				log(NOTICE, "Failpoint: "
				    "siq_exit_after_job_version_set"
				    " (restart not expected)");
				exit(EXIT_FAILURE);
			} else {
				/* Avoids atexit()-registered routines &
				 * results in a job restart. */
				log(NOTICE, "Failpoint: "
				    "siq_exit_after_job_version_set"
				    " (restart expected)");
				_Exit(EXIT_SUCCESS);
			}
		});
	}

	/* Register callbacks */
	migr_add_fd(tctx->sock, &g_ctx, "tmonitor");
	migr_register_callback(tctx->sock, DISCON_MSG,
	    tmonitor_disconnect_callback);
	migr_register_callback(tctx->sock, TARGET_CANCEL_MSG,
	    target_cancel_callback);
	migr_register_callback(tctx->sock, TARGET_MONITOR_SHUTDOWN_MSG,
	    tmonitor_shutdown_callback);
	migr_register_callback(tctx->sock, SIQ_ERROR_MSG,
	    siq_error_callback);
	migr_register_timeout(&tmonitor_timeout,
	    tmonitor_update_timer_callback, 0);

	g_ctx.new_tgt_nodes = calloc(1, sizeof(struct nodes));
	ASSERT(g_ctx.new_tgt_nodes);
	g_ctx.new_tgt_nodes->ips = ip_list;		
	g_ctx.new_tgt_nodes->count = n_target_nodes;

	for (i = 0; i < g_ctx.new_tgt_nodes->count; i++) {
		/* Add to a string of the remote's IPs */
		fmt_print(&target_fmt, "%d)%s ", i + 1, ip_list[i]);
	}

	if (JOB_SPEC->target->target_restrict &&
	    (inet_pton(AF_INET, g_ctx.rc.target, &addr4) <= 0) &&
	    (inet_pton(AF_INET6, g_ctx.rc.target, &addr6) <= 0)) {
		/* Message for target_restrict with non-IP target */
		log(NOTICE, "Target is restricted to the %d nodes with IPs: %s",
		    g_ctx.new_tgt_nodes->count, fmt_string(&target_fmt));
	} else {
		log(NOTICE, "%d nodes on target available: %s",
		    g_ctx.new_tgt_nodes->count, fmt_string(&target_fmt));
	}

out:
	isi_error_handle(error, error_out);
}

/*
 * Send a status update to the target monitor.
 */
void
send_status_update_to_tmonitor()
{
	struct generic_msg js = {};

	if (g_ctx.tctx.sock > 0) {
		log(DEBUG, "%s: Sending tmonitor job status %s", __func__,
		    job_state_to_text(JOB_SUMM->total->state, false));
		js.head.type = JOB_STATUS_MSG;
		js.body.job_status.job_status = JOB_SUMM->total->state;

		/* If target is pre-riptide, and this job was preempted, tell
		 * the target that the job is paused. It doesn't know about
		 * the preempted status, and paused is the same concept. */
		if (g_ctx.job_ver.common < MSG_VERSION_RIPTIDE &&
		    js.body.job_status.job_status == SIQ_JS_PREEMPTED)
			js.body.job_status.job_status = SIQ_JS_PAUSED;

		msg_send(g_ctx.tctx.sock, &js);
	}
}

enum tgt_send_result
send_tgt_msg_with_retry(struct generic_msg *m, uint32_t resp_type,
    migr_callback_t resp_cb)
{
	int i;
	migr_callback_t old_cb = NULL;
	enum tgt_send_result res = TGT_SEND_OK;

	g_ctx.waiting_for_tgt_msg = true;
	old_cb = migr_get_registered_callback(g_ctx.tctx.sock, resp_type);

	migr_register_callback(g_ctx.tctx.sock, resp_type, resp_cb);

	for (i = 0; i < MAX_TGT_TRIES && g_ctx.waiting_for_tgt_msg; i++) {
		msg_send(g_ctx.tctx.sock, m);
		if (migr_process() < 0) {
			set_job_state(SIQ_JS_FAILED);
			res = TGT_SEND_MIGR_PROCESS_ERR;
			goto out;
		}
	}

	if (i == MAX_TGT_TRIES)
		res = TGT_SEND_FAIL;

out:
	migr_register_callback(g_ctx.tctx.sock, resp_type, old_cb);

	return res;
}

void
disconnect_from_tmonitor()
{
	tmonitor_disconnect(&g_ctx.tctx);
}
