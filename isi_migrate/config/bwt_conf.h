#ifndef __BWT_CONF_H__
#define __BWT_CONF_H__

#include "isi_migrate/config/siq_util.h"
#include "isi_migrate/config/siq_gconfig_gcfg.h"
#include "isi_migrate/siq_error.h"

#ifdef __cplusplus
extern "C" {
#endif

#define KEYFILE_OPEN_ERR "Failed to fopen keyfile"

/* TYPES */

enum bwt_limit_type {
	BWT_LIM_BW = 0,
	BWT_LIM_TH,
	BWT_LIM_CPU,
	BWT_LIM_WORK,
	BWT_LIM_TOTAL
};

struct bwt_gc_conf {
	struct bwt_conf *root;
	struct gci_base *gci_base;
	struct gci_ctx *gci_ctx;
};

typedef struct bwt_report_node {
	char* node;
	long usage;
	struct bwt_report_node* next;
} bwt_report_node_t;

typedef struct bwt_report {
	/* Report timestamp */
	time_t timestamp;
	/* Report type (Throttle, Bandwidth or CPU) */
	enum bwt_limit_type type;
	/* Limit defined by BW/TH/CPU rules */
	long limit;
	/* Amount allocated by daemon to workers */
	long allocated;
	/* Total usage reported by workers */
	long total;
	/* Usage per node */
	struct bwt_report_node* node_usage;
	/* Next report */
	struct bwt_report* next;
} bwt_report_t;

extern struct gci_tree bwt_gc_conf_gci_root;

/* PROTOTYPES */

/* All the methods below use the error reporting mechanism described in
 * [[TSM Error Reporting API]] on wiki.  This means that each method which can
 * fail somehow _notifies_ that an error occurred.  The detailed error reports
 * can be got by the analisys of siq_error_old global variable */

struct bwt_conf *bwt_conf_create(void);
struct bwt_limit *bwt_limit_create(void);

/* Load global BW/T configuration.  If @context is NULL then the default
 * context is taken.  Returns NULL in case of an error */
struct bwt_gc_conf *bwt_gc_conf_load(struct isi_error **error_out);

/* Save global BW/T configuration.  Returns 0 in case of success, < 0 in case
 * of an error */
int bwt_gc_conf_save(struct bwt_gc_conf *conf, struct isi_error **error_out);

/* Free BW/T configuration structure */
void bwt_gc_conf_close(struct bwt_gc_conf* conf);
void bwt_gc_conf_free(struct bwt_gc_conf* conf);

void bwt_conf_free(struct bwt_conf *conf);
void bwt_limit_free(struct bwt_limit_head *head);

/* Get reports for a given time period */
bwt_report_t* bwt_bw_reports_get(time_t start, time_t end, 
    struct isi_error** error_out);
bwt_report_t* bwt_th_reports_get(time_t start, time_t end, 
    struct isi_error** error_out);
bwt_report_t* bwt_cpu_reports_get(time_t start, time_t end, 
    struct isi_error** error_out);
bwt_report_t* bwt_work_reports_get(time_t start, time_t end,
    struct isi_error** error_out);

/* Free BW/T report structure */
void bwt_report_node_free(bwt_report_node_t* node);
void bwt_reports_free(bwt_report_t* report);

#ifdef __cplusplus
}
#endif

#endif /* __BWT_CONF_H__ */
