#include "siq_conf.h"
#include "siq_report.h"
#include "isi_migrate/migr/isirep.h"

unsigned DENY_FALLBACK_MIN_VERSION = MSG_VERSION_CHOPUVILLE;
int CONNECT_TIMEOUT = 15;

int
get_deny_fallback_min_ver(void)
{
	return DENY_FALLBACK_MIN_VERSION;
}

void
override_connect_timeout(int timeout)
{
	CONNECT_TIMEOUT = (timeout > 0) ? timeout : 0;
}

int
get_connect_timeout(void)
{
	return CONNECT_TIMEOUT;
}

struct gci_tree siq_gc_conf_gci_root = {
	.root = &gci_ivar_siq_conf,
	.primary_config = "/ifs/.ifsvar/modules/tsm/config/siq-conf.gc",
	.fallback_config_ro = NULL,
	.change_log_file = NULL
};

struct siq_conf*
siq_conf_create_or_populate(struct siq_conf *conf)
{
	CREATE_IF_NULL(conf->application, struct siq_conf_application);
	CREATE_IF_NULL(conf->coordinator, struct siq_conf_coordinator);
	CREATE_IF_NULL(conf->coordinator->ports, 
	    struct siq_conf_coordinator_ports);
	CREATE_IF_NULL(conf->bwt, struct siq_conf_bwt);
	CREATE_IF_NULL(conf->scheduler, struct siq_conf_scheduler);
	CREATE_IF_NULL(conf->reports, struct siq_conf_reports);
	
	return conf;
}

struct siq_gc_conf*
siq_gc_conf_load(struct isi_error **error_out)
{
	struct siq_gc_conf *conf = NULL;
	struct isi_error *error = NULL;
	
	CALLOC_AND_ASSERT(conf, struct siq_gc_conf);
	
	conf->gci_base = gcfg_open(&siq_gc_conf_gci_root, 0, &error);
	if (error) {
		log(ERROR, "%s: Failed to gcfg_open: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	conf->gci_ctx = gci_ctx_new(conf->gci_base, false, &error);
	if (error) {
		log(ERROR, "%s: Failed to gci_ctx_new: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	gci_ctx_read_path(conf->gci_ctx, "", &conf->root, &error);
	if (error) {
		log(ERROR, "%s: Failed to gci_ctx_read_path: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	
	conf->root = siq_conf_create_or_populate(conf->root);
	
	if (conf->root->application->deny_fallback_min_ver_override != 0) {
		DENY_FALLBACK_MIN_VERSION = 
		    conf->root->application->deny_fallback_min_ver_override;
	}
	
	if (conf->root->application->connect_timeout != 0) {
		override_connect_timeout(
		    conf->root->application->connect_timeout);
	}
	
out:
	if (error) {
		siq_gc_conf_free(conf);
		isi_error_handle(error, error_out);
		conf = NULL;
	}
	return conf;
}

int 
siq_gc_conf_save(struct siq_gc_conf *conf, struct isi_error **error_out)
{
	int ret = -1;
	struct isi_error *isierror = NULL;
	
	ASSERT(conf->gci_ctx && conf->gci_base);
	
	gci_ctx_write_path(conf->gci_ctx, "", conf->root, &isierror);
	if (isierror) {
		log(ERROR, "%s: Failed to gci_ctx_write_path: %s", __func__, 
		    isi_error_get_message(isierror));
		goto out;
	}
	
	gci_ctx_commit(conf->gci_ctx, &isierror);
	if (isierror) {
		log(ERROR, "%s: Failed to gci_ctx_commit: %s", __func__, 
		    isi_error_get_message(isierror));
		goto out;
	}
	
	gci_ctx_free(conf->gci_ctx);
	conf->gci_ctx = gci_ctx_new(conf->gci_base, false, &isierror);
	if (isierror) {
		log(ERROR, "%s: Failed to gci_ctx_new: %s", __func__, 
		    isi_error_get_message(isierror));
		goto out;
	}

	siq_conf_free(conf->root);
	gci_ctx_read_path(conf->gci_ctx, "", &conf->root, &isierror);
	if (isierror) {
		log(ERROR, "%s: Failed to gci_ctx_read_path: %s", __func__, 
		    isi_error_get_message(isierror));
		goto out;
	}
	
	conf->root = siq_conf_create_or_populate(conf->root);
	
	if (conf->root->application->deny_fallback_min_ver_override != 0) {
		DENY_FALLBACK_MIN_VERSION = 
		    conf->root->application->deny_fallback_min_ver_override;
	}
	
	if (conf->root->application->connect_timeout != 0) {
		override_connect_timeout(
		    conf->root->application->connect_timeout);
	}
	
	ret = 0;

out:
	isi_error_handle(isierror, error_out);
	return ret;
}

void
siq_conf_free(struct siq_conf *conf)
{
	if (!conf) {
		return;
	}
	
	FREE_AND_NULL(conf->application);
	if (conf->coordinator) {
		FREE_AND_NULL(conf->coordinator->ports);
		siq_policy_path_free(&conf->coordinator->paths);
		FREE_AND_NULL(conf->coordinator->passwd_file);
		FREE_AND_NULL(conf->coordinator->snapshot_pattern);
		FREE_AND_NULL(conf->coordinator->default_restrict_by);
		FREE_AND_NULL(conf->coordinator);
	}
	if (conf->bwt) {
		FREE_AND_NULL(conf->bwt->host);
		FREE_AND_NULL(conf->bwt->bw_key_file);
		FREE_AND_NULL(conf->bwt->th_key_file);
		FREE_AND_NULL(conf->bwt->cpu_key_file);
		FREE_AND_NULL(conf->bwt->worker_key_file);
		FREE_AND_NULL(conf->bwt);
	}
	if (conf->scheduler) {
		FREE_AND_NULL(conf->scheduler->pid_file);
		FREE_AND_NULL(conf->scheduler);
	}
	if (conf->reports) {
		FREE_AND_NULL(conf->reports->rotation_period);
		siq_string_list_free(&conf->reports->addresses);
		FREE_AND_NULL(conf->reports);
	}
	
	FREE_AND_NULL(conf);
}

void 
siq_gc_conf_close(struct siq_gc_conf *conf)
{
	if (!conf) {
		return;
	}
	
	if (conf->gci_ctx) {
		gci_ctx_free(conf->gci_ctx);
		conf->gci_ctx = NULL;
	}
	if (conf->gci_base) {
		gcfg_close(conf->gci_base);
		conf->gci_base = NULL;
	}
}

void 
siq_gc_conf_free(struct siq_gc_conf *conf)
{
	if (!conf) {
		return;
	}
	
	siq_gc_conf_close(conf);
	siq_conf_free(conf->root);
	
	FREE_AND_NULL(conf);
}

void
siq_print_version(const char *name)
{
	printf("Isilon %s %d.%d.%d %s build %s\n",
               SIQ_APP_NAME,
               SIQ_APP_VER_MAJOR,
               SIQ_APP_VER_MIDDLE,
               SIQ_APP_VER_MINOR,
               name, BUILD);
}

char *
siq_state_to_text(enum siq_state state)
{
	struct fmt FMT_INIT_CLEAN(fmt);
	char *to_return = NULL;
	
	fmt_print(&fmt, "%{}", enum_siq_state_fmt(state));
	asprintf(&to_return, "%s", fmt_string(&fmt));
	fmt_clean(&fmt);
	
	return to_return;
}

