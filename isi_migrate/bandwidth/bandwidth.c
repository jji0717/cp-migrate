#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/param.h>
#include <errno.h>
#include <isi_ufp/isi_ufp.h>

#include "isi_flexnet/isi_flexnet.h"
#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/config/siq_conf.h"
#include "isi_migrate/config/bwt_conf.h"
#include "isi_stats/isi_stats_api.h"
#include "isi_stats/isi_stats_keys.h"
#include "isi_stats/isi_stats_error.h"
#include "isi_migrate/migr/siq_workers_utils.h"

#define MIN_WORKER_CPU_RATION 1		/* 0.1% */
#define ISI_STATS_TIMEOUT 10		//TODO: is this reasonable?
#define MAX_KEY_LEN 40			/* max null-terminated ipv6 length */
#define MAX_WORKERS_PER_CPU 20		/* hard max workers per cpu */

static struct timeval timeout = {10, 0};
static void *group_retry_tout = NULL;
static int num_nodes = 0;
static int max_wper_cluster = 0;
static int max_wper_node = 0;
static int max_wper_job = 0;
static int set_wper_node = -1;
static int wper_cpu = 4;
static int max_wpn_per_job = 8;

static struct {
	int		devid;
	char *		ip; /* This node's internal IP address */
	bool		master;	/* active (master) host of the cluster */
	struct version_status ver_stat;
	int		master_fd; /* socket to the master host */
} g_ctx;

struct worker {
	int socket;
	enum worker_type type;
	uint64_t	cpu_time;	/* usec */
	uint64_t	time;		/* usec */
};

struct wp_node {
	int		lnn;
	int		max_workers;
	int		rationed_workers;
};

struct context {
	char *		type;
	char *		unit;
	enum bwt_limit_type bwt_type;
	struct ptr_vec	jobs;
	struct ptr_vec	hosts;
	struct ptr_vec	timeslots;
	int		worker_total;
	int		ration;
	int		pool;
	FILE		*log;
	time_t		btime;	/* Log birth timestamp */

	/* used for cpu throttling only */
	int		pworker_total;
	int		sworker_total;
	int		p_hasher_total;
	int		s_hasher_total;
	struct		ptr_vec workers;
	int		adjusted_ration;
	uint32_t	per_worker_ration;

	/* used for worker pool only */
	struct uint16_set	isi_stats_keys;
	bool		update_node_info;
	bool		update_node_rations;
	int		total_rationed_workers;
	int		per_job_ration;
	int		remainder_workers;
	struct ptr_vec	wp_nodes;
};

struct host {
	char		name[MAXHOSTNAMELEN];
	uint64_t	usage;
	int		socket;
};

struct context bw_ctx = {"bandwidth", "kbps", BWT_LIM_BW};////coord连接master node
struct context th_ctx = {"throttle", "fps", BWT_LIM_TH};
struct context cpu_ctx = {"cpu", "percent", BWT_LIM_CPU}; //////cpu_throttle: pworker/sworker 连接bandwidth
struct context work_ctx = {"worker", "percent", BWT_LIM_WORK};

static struct siq_gc_conf *siq_gc = NULL;
static struct bwt_gc_conf *siq_bc = NULL;

struct timeslot {
	char		days;
	short		start;
	short		end;
	int		limit;
};

struct job {
	char		name[MAXNAMELEN];
	int		num_workers;
	int		priority;
	int		socket;
	int		ration;
	uint64_t	usage;
	struct context *ctx;

	/* used by reallocation algorithm */
	double		weight;
	double		target;
	int		last_ration;

	/* used for worker pool only */
	char		possible_nodes[SIQ_MAX_NODES + 1];
	char		allowed_nodes[SIQ_MAX_NODES + 1];
	int	requested_workers;
};

static int timer_cb(void *);
static void conf_check(struct context *, bool reload);
static char *log_file_name(struct context *ctx);
static char *key_file_name(struct context *ctx);
static int bandwidth_stat_cb(struct generic_msg *m, void *ctx);
static int throttle_stat_cb(struct generic_msg *m, void *ctx);
static int cpu_throttle_stat_cb(struct generic_msg *m, void *ctx);
static int uninit_disconnect_cb(struct generic_msg *m, void *ctx);
static int bw_or_fps_disconnect_cb(struct generic_msg *m, void *ctx);
static int cpu_disconnect_cb(struct generic_msg *m, void *ctx);
static int worker_stat_cb(struct generic_msg *m, void *ctx);
static int worker_node_update_cb(struct generic_msg *m, void *ctx);
static int worker_disconnect_cb(struct generic_msg *m, void *ctx);
static int master_disconnect_cb(struct generic_msg *m, void *ctx);
static struct host * find_or_add_host(struct context *ctx, char *name);
static struct host * update_node_cpu_usage(char *ip, int usage_pct,
    struct isi_error **error_out);
static int cpu_node_usage_cb(struct generic_msg *m, void *ctx);
static int cpu_usage_disconnect_cb(struct generic_msg *m, void *ctx);
static void reload_global_conf(bool load_only);

static void
print_work_nodes(FILE* f, char *nodes)
{
	int counter = 0;
	for (counter = 0; counter < SIQ_MAX_NODES; counter++) {
		if (nodes[counter] == '\0') {
			break;
		}
		if (nodes[counter] == '.') {
			fprintf(f, "%d ", counter);
		}
	}
}

static void
print_context(FILE* f, struct context *ctx)
{
	struct job *cur_job = NULL;
	int job_size = 0;
	struct host *cur_host = NULL;
	int host_size = 0;
	struct timeslot *cur_ts = NULL;
	int ts_size = 0;
	struct worker *cur_w = NULL;
	int w_size = 0;
	void **pvec_item = NULL;
	struct wp_node *cur_wpn = NULL;
	
	fprintf(f, "type: %s\n", ctx->type);
	fprintf(f, "unit: %s\n", ctx->unit);
	job_size = pvec_size(&ctx->jobs);
	fprintf(f, "jobs size: %d\n", job_size);
	if (job_size > 0) {
		fprintf(f, "jobs:\n");
		fprintf(f, "  ---\n");
		PVEC_FOREACH(pvec_item, &ctx->jobs) {
			cur_job = (struct job *)*pvec_item;
			fprintf(f, "  name: %s\n", cur_job->name);
			fprintf(f, "  num_workers: %d\n", cur_job->num_workers);
			fprintf(f, "  priority: %d\n", cur_job->priority);
			fprintf(f, "  socket: %d\n", cur_job->socket);
			fprintf(f, "  ration: %d\n", cur_job->ration);
			fprintf(f, "  name: %lld\n", cur_job->usage);
			fprintf(f, "  weight: %f\n", cur_job->weight);
			fprintf(f, "  target: %f\n", cur_job->target);
			fprintf(f, "  last_ration: %d\n", cur_job->last_ration);
			if (ctx->bwt_type == BWT_LIM_WORK) {
				fprintf(f, "  possible_nodes: ");
				print_work_nodes(f, cur_job->possible_nodes);
				fprintf(f, "\n");
				fprintf(f, "  allowed_nodes: ");
				print_work_nodes(f, cur_job->allowed_nodes);
				fprintf(f, "\n");
				fprintf(f, "  requested_workers: %d\n",
				    cur_job->requested_workers);
			}
			fprintf(f, "  ---\n");
		}
	}
	host_size = pvec_size(&ctx->hosts);
	fprintf(f, "hosts size: %d\n", host_size);
	if (host_size > 0) {
		fprintf(f, "hosts:\n");
		fprintf(f, "  ---\n");
		PVEC_FOREACH(pvec_item, &ctx->hosts) {
			cur_host = (struct host *)*pvec_item;
			fprintf(f, "  name: %s\n", cur_host->name);
			fprintf(f, "  usage: %lld\n", cur_host->usage);
			fprintf(f, "  ---\n");
		}
	}
	ts_size = pvec_size(&ctx->timeslots);
	fprintf(f, "timeslots size: %d\n", ts_size);
	if (ts_size > 0) {
		fprintf(f, "timeslots:\n");
		fprintf(f, "  ---\n");
		PVEC_FOREACH(pvec_item, &ctx->timeslots) {
			cur_ts = (struct timeslot *)*pvec_item;
			fprintf(f, "  days: %d\n", cur_ts->days);
			fprintf(f, "  start: %d\n", cur_ts->start);
			fprintf(f, "  end: %d\n", cur_ts->end);
			fprintf(f, "  limit: %d\n", cur_ts->limit);
			fprintf(f, "  ---\n");
		}
	}
	
	fprintf(f, "worker_total: %d\n", ctx->worker_total);
	fprintf(f, "ration: %d\n", ctx->ration);
	fprintf(f, "pool: %d\n", ctx->pool);
	fprintf(f, "btime: %.24s (%lld)\n", ctime(&ctx->btime), ctx->btime);
	
	if (ctx->bwt_type == BWT_LIM_CPU) {
		fprintf(f, "pworker_total: %d\n", ctx->pworker_total);
		fprintf(f, "sworker_total: %d\n", ctx->sworker_total);
		fprintf(f, "p_hasher_total: %d\n", ctx->p_hasher_total);
		fprintf(f, "s_hasher_total: %d\n", ctx->s_hasher_total);
		w_size = pvec_size(&ctx->workers);
		fprintf(f, "workers size: %d\n", w_size);
		fprintf(f, "workers:\n");
		fprintf(f, "  ---\n");
		PVEC_FOREACH(pvec_item, &ctx->workers) {
			cur_w = (struct worker *)*pvec_item;
			fprintf(f, "  cpu_time: %lld\n", cur_w->cpu_time);
			fprintf(f, "  time: %lld\n", cur_w->time);
			fprintf(f, "  ---\n");
		}
		fprintf(f, "adjusted_ration: %d\n", ctx->adjusted_ration);
		fprintf(f, "per_worker_ration: %d\n", ctx->per_worker_ration);
	}

	if (ctx->bwt_type == BWT_LIM_WORK) {
		fprintf(f, "num_nodes: %d\n", num_nodes);
		fprintf(f, "max_wper_cluster: %d\n", max_wper_cluster);
		fprintf(f, "max_wper_node: %d\n", max_wper_node);
		fprintf(f, "max_wper_job: %d\n", max_wper_job);
		fprintf(f, "set_wper_node: %d\n", set_wper_node);
		fprintf(f, "wper_cpu: %d\n", wper_cpu);
		fprintf(f, "max_wpn_per_job: %d\n", max_wpn_per_job);
		fprintf(f, "total_rationed_workers: %d\n",
		    ctx->total_rationed_workers);
		fprintf(f, "per_job_ration: %d\n",
		    ctx->per_job_ration);
		fprintf(f, "remainder_workers: %d\n",
		    ctx->remainder_workers);
		fprintf(f, "wp_nodes:\n");
		fprintf(f, "  ---\n");
		PVEC_FOREACH(pvec_item, &ctx->wp_nodes) {
			cur_wpn = (struct wp_node *)*pvec_item;
			fprintf(f, "  lnn: %d\n", cur_wpn->lnn);
			fprintf(f, "  max_workers: %d\n", cur_wpn->max_workers);
			fprintf(f, "  ---\n");
		}
	}
}

static int
dump_status(void *ctx)
{
	char *output_file = NULL;
	FILE *f = NULL;
	int ret = 1;
	
	asprintf(&output_file, "/var/tmp/bwt_status_%d.txt", getpid());
	f = fopen(output_file, "w");
	if (f == NULL) {
		log(ERROR,"Status generation failed: %s", strerror(errno));
		goto out;
	}
	
	fprintf(f, "****Global Context****\n");
	fprintf(f, "devid: %d\n", g_ctx.devid);
	fprintf(f, "master: %s\n", PRINT_BOOL(g_ctx.master));
	fprintf(f, "\n****Bandwidth Context****\n");
	print_context(f, &bw_ctx);
	fprintf(f, "\n****Throttle Context****\n");
	print_context(f, &th_ctx);
	fprintf(f, "\n****CPU Context****\n");
	print_context(f, &cpu_ctx);
	fprintf(f, "\n****Worker Context****\n");
	print_context(f, &work_ctx);
	
	print_net_state(f);
	
	log(NOTICE, "Status information dumped to %s", output_file);
	ret = 0;
out:
	if (f) {
		fclose(f);
		f = NULL;
	}
	free(output_file);
	return ret;
}

static void
sigusr2(int signo)
{
	struct timeval tv = {};

	migr_register_timeout(&tv, dump_status, NULL);
}

/*  _                  __  __                 _   
 * | |    ___   __ _  |  \/  | __ _ _ __ ___ | |_ 
 * | |   / _ \ / _` | | |\/| |/ _` | '_ ` _ \| __|
 * | |__| (_) | (_| | | |  | | (_| | | | | | | |_ 
 * |_____\___/ \__, | |_|  |_|\__, |_| |_| |_|\__|
 *             |___/          |___/               
 */

static FILE *
open_log(struct context *ctx)
{
	char path[MAXPATHLEN + 1];
	struct stat stat;

	ASSERT(ctx && siq_gc);

	if (ctx->log)
		fclose(ctx->log);

	snprintf(path, MAXPATHLEN + 1, "%s/%s", SIQ_BWT_WORKING_DIR,
	    log_file_name(ctx));

	if ((ctx->log = fopen(path, "a"))) {
		if (fstat(fileno(ctx->log), &stat) < 0) {
			log(ERROR, "Failed to stat %s log", ctx->type);
			fclose(ctx->log);
			ctx->log = NULL; 
			ctx->btime = 0;
		} else
			ctx->btime = stat.st_birthtime;
	} else
		log(ERROR, "Failed to open %s log", ctx->type);

	return (ctx) ? ctx->log : NULL;
}

static void
rotate_log(struct context *ctx)
{
	time_t ctime;
	char cname[MAXPATHLEN + 1], sname[MAXPATHLEN + 1];
	int ret = -1;

	ASSERT(ctx && siq_gc);

	if ((!ctx->log || !ctx->btime) && open_log(ctx) == NULL)
		goto out;

	if ((ctime = time(0)) - ctx->btime >= 
	    siq_gc->root->bwt->log_rotate_period) {
		if (ctx->log) {
			fclose(ctx->log);
			ctx->log = NULL;
		}

		snprintf(sname, MAXPATHLEN + 1, "%s/%s-%ld.log",
			SIQ_BWT_WORKING_DIR, ctx->type, ctime);
		snprintf(cname, MAXPATHLEN + 1, "%s/%s", 
			SIQ_BWT_WORKING_DIR, log_file_name(ctx));
		ret = rename(cname, sname);
		if (ret == -1) {
			log(ERROR, "failed to rotate %s -> %s (%d)",
			    cname, sname, errno);
		} else {
			log(INFO, "rotated %s -> %s", cname, sname);
		}

		open_log(ctx);
	}

out:
	return;
}

/*  _   _ _   _ _     
 * | | | | |_(_) |___ 
 * | | | | __| | / __|
 * | |_| | |_| | \__ \
 *  \___/ \__|_|_|___/
 */

static char *
log_file_name(struct context *ctx)
{
	ASSERT(ctx);
	if (ctx == &bw_ctx)
		return SIQ_BWT_BW_LOG_FILE;
	else if (ctx == &th_ctx)
		return SIQ_BWT_TH_LOG_FILE;
	else if (ctx == &cpu_ctx)
		return SIQ_BWT_CPU_LOG_FILE;
	else
		return SIQ_BWT_WORKER_LOG_FILE;
}

static char *
key_file_name(struct context *ctx)
{
	ASSERT(ctx);
	if (ctx == &bw_ctx)
		return siq_gc->root->bwt->bw_key_file;
	else if (ctx == &th_ctx)
		return siq_gc->root->bwt->th_key_file;
	else if (ctx == &cpu_ctx)
		return siq_gc->root->bwt->cpu_key_file;
	else
		return siq_gc->root->bwt->worker_key_file;
}

static void
load_key_hosts(struct context *ctx)
{
	FILE *keyfile = NULL;
	char path[MAXPATHLEN + 1], name[MAXHOSTNAMELEN + 1];
	struct host *h = NULL;

	/* load existing list of hosts and populate host list */
	snprintf(path, MAXPATHLEN + 1, "%s/%s", SIQ_BWT_WORKING_DIR,
	    key_file_name(ctx));
	if ((keyfile = fopen(path, "r"))) {
		while (fgets(name, MAXHOSTNAMELEN + 1, keyfile)) {
			if (name[strlen(name) - 1] != '\n') {
				log(ERROR, "error processing %s",
				    key_file_name(ctx));
				break;
			}
			name[strlen(name) - 1] = 0;
			h = calloc(1, sizeof(struct host));
			memcpy(h->name, name, strlen(name));
			pvec_append(&ctx->hosts, (void *)h);
		}
		fclose(keyfile);
	}
}

static void
load_cpu_hosts(struct context *ctx)
{
	char **ip_list = NULL;
	int n_nodes = -1, i;
	struct isi_error *error = NULL;

	n_nodes = get_local_cluster_info(NULL, &ip_list, NULL, NULL, 0, NULL, 
	    true, true, NULL, &error);
	if (error || n_nodes <= 0) {
		log(ERROR, "Failed to init load cpu hosts. %s",
		    error ? isi_error_get_message(error) : "Unable to"
		    " get local cluster ips");
		goto out;
	}

	/* Create a host for each node in the cluster */
	for (i = 0; ip_list[i]; i++)
		find_or_add_host(ctx, ip_list[i]);

out:
	if (ip_list)
		free_ip_list(ip_list);
	isi_error_free(error);

}

static void
init_ctx(struct context *ctx)
{

	log(TRACE, "init_ctx %s", ctx->type);
	pvec_init(&ctx->jobs);
	pvec_init(&ctx->hosts);
	pvec_init(&ctx->timeslots);
	pvec_init(&ctx->workers);
	/* worker pool specific init */
	pvec_init(&ctx->wp_nodes);

	if (!strcmp(ctx->type, "cpu")) {
		load_cpu_hosts(ctx);	
	} else {
		load_key_hosts(ctx);
	}
}

static void
send_ration_update(struct job *job)
{
	struct generic_msg msg;
	int delta;
	struct context *ctx = job->ctx;

	ASSERT(job->socket != -1);

	log(DEBUG, "update_job_ration: %s ration of %d %s sent to job %s",
	    ctx->type, job->ration, ctx->unit, job->name);

	delta = job->ration - job->last_ration;
	job->last_ration = job->ration;
	ctx->pool -= delta;

	if (ctx == &bw_ctx) {
		msg.head.type = BANDWIDTH_MSG;
		msg.body.bandwidth.ration = job->ration;
		msg_send(job->socket, &msg);
	} else {
		msg.head.type = THROTTLE_MSG;
		msg.body.throttle.ration = job->ration;
		msg_send(job->socket, &msg);
	}
}

/* This divides a bw or fps ration across jobs based on how many workers each
 * job has - jobs with more workers get a larger slice of the ration. This
 * should be considered legacy behavior, and is only to be used if worker
 * pools is disabled. */
static void
divide_bw_fps_ration_by_weight(struct context *ctx)
{
	void **ptr;
	struct job *job;
	int quotient, remainder = 0;
	double weight;

	if (ctx->worker_total == 0)
		return;

	/* first, calcualte a quotient which represents an even number that can
	 * be allocated to each worker.
	 *
	 *	quotient = total_ration / total_workers
	 *
	 * for each job, we calculate its ration as follows:
	 *
	 *	job_ration = quotient * job_workers
	 *
	 * at this point, each job has an initial ration value that is exactly 
	 * proportional to its weight:
	 *
	 *	job_weight = job_workers / total_workers
	 */

	quotient = ctx->ration / ctx->worker_total;

	PVEC_FOREACH(ptr, &ctx->jobs) {
		job = *ptr;

		/* calculate the weight and target ration for the job */
		weight = (double)job->num_workers / (double)ctx->worker_total;
		job->target = weight * (double)ctx->ration;

		job->ration = quotient * job->num_workers;

		/* subtract the ration just given from the job's target 
		 * ration */
		job->target -= (double)job->ration;

		/*
		 * The mixing of integer math with floating point math can
		 * cause target to go negative here.  Make sure that doesn't
		 * happen because it causes hell in the next loop.
		 * See bug 70223
		 */
		if (job->target < 0.0)
			job->target = 0.0;
	}

	/* now, we are left with a remainder, which is less than the total
	 * worker count.
	 *
	 * assuming the quotient was non-zero, we could just distribute the 
	 * ration roughly based upon job weight. but if the quotion were zero,
	 * we could potentially starve one or more jobs.
	 *
	 * in the previous step, we calculated a "target" ration for each job
	 * that represents its ideal (probably non-integer) share of the total
	 * ration:
	 *
	 * 	job_target = job_weight * total_ration
	 *
	 * we subtracted from it the ration it was given in the first step:
	 *
	 *	job_target -= job_ration
	 *
	 * the job's target now equals its ideal (non-integer) share of the 
	 * remainder.
	 *
	 * now we enter a while loop that continues until the remainder is used
	 * up. on each trip through the while loop, for each job, if the job's 
	 * target is greater than zero, we increment its 
	 * ration, and decrement its target and the remainder. */

	remainder = ctx->ration % ctx->worker_total;

	while (remainder > 0) {
		PVEC_FOREACH(ptr, &ctx->jobs) {
			job = *ptr;

			if (remainder == 0)
				break;

			if (job->target > 0.0) {
				job->ration++;
				job->target = job->target - 1.0;
				remainder--;
			}
		}
	}


}

/* This divides a bw or fps ration evenly amongst all jobs. With worker pools
 * enabled, this is the default behavior - we can assume that each job will
 * eventually have the same amount of workers, so their rations should just be
 * an even slice of the total ration. */
static void
divide_bw_fps_ration_evenly(struct context *ctx)
{
	void **ptr;
	struct job *job;
	int per_job_ration, remainder, num_jobs;

	num_jobs = pvec_size(&ctx->jobs);
	if (num_jobs <= 0) {
		return;
	}
	per_job_ration = ctx->ration / num_jobs;
	remainder = ctx->ration % num_jobs;
	PVEC_FOREACH(ptr, &ctx->jobs) {
		job = *ptr;
		job->ration = per_job_ration;
		if (remainder) {
			job->ration++;
			remainder--;
		}
	}
}


static void
reallocate_bw_or_fps(struct context *ctx)
{
	void **ptr;
	struct job *job;

	/* special case if ration is 0 (none) or -1 (infinite). just send the
	 * value to each job */
	if (ctx->ration <= 0) {
		PVEC_FOREACH(ptr, &ctx->jobs) {
			job = *ptr;
			job->ration = ctx->ration;
			send_ration_update(job);
		}
		goto out;
	}

	/* special case if ration is less than the total job count. to avoid
	 * starvation we give all jobs a ration of 1 */
	if (ctx->ration < pvec_size(&ctx->jobs)) {
		PVEC_FOREACH(ptr, &ctx->jobs) {
			job = *ptr;
			job->ration = 1;
			send_ration_update(job);
		}
		goto out;
	}

	if (siq_gc->root->coordinator->skip_work_host) {
		divide_bw_fps_ration_by_weight(ctx);
	} else {
		divide_bw_fps_ration_evenly(ctx);////////ctx->ration / jos_num.   job->ration: 平均分所有的ration
	}

	/* send the updated job rations to each coordinator */
	PVEC_FOREACH(ptr, &ctx->jobs) {/////发送给每一个coord(一个job可以看作是一个coord)
		job = *ptr;
		send_ration_update(job);
	}

out:
	return;
}

static void
reallocate_cpu(struct context *ctx)
{
	void **ptr;
	struct generic_msg msg = {};
	uint32_t per_worker_ration;
	int num_workers = pvec_size(&ctx->workers);

	if (num_workers == 0)
		goto out;

	if (ctx->ration == 100 || ctx->ration == -1) {
		/* if the configured limit is unlimited (-1) or 100% we don't
		 * want to try to divide it up among workers. let them all run
		 * at full throttle */
		per_worker_ration = 1000;
	} else {
		/* adjusted ration is limited to num_workers * ration */
		if (ctx->adjusted_ration > num_workers * ctx->ration)
			ctx->adjusted_ration = num_workers * ctx->ration;

		/* calculate new per worker ration. the ration on the wire
		 * is in cycles per thousand */ 
		per_worker_ration = ctx->adjusted_ration * 10 / num_workers;
		 
		/* if the calculated ration is too low, bump it ration up to
		 * the minimum */
		if (per_worker_ration < MIN_WORKER_CPU_RATION)
			per_worker_ration = MIN_WORKER_CPU_RATION;

		if (per_worker_ration > 1000)
			per_worker_ration = 1000;
	}

	log(DEBUG, "worker cpu ration: %d", per_worker_ration);
	ctx->per_worker_ration = per_worker_ration;

	msg.head.type = CPU_THROTTLE_MSG;
	msg.body.cpu_throttle.ration = per_worker_ration;

	/* send new ration to each registered worker */
	PVEC_FOREACH(ptr, &ctx->workers) {
		msg_send(((struct worker *)*ptr)->socket, &msg);
	}

out:
	return;
}

static int
get_workers(struct stats_result_set *stats, int devid)
{
	int num_cores = 0;
	//int num_disks = 0;
	struct stats_result *res = NULL;
	int to_return = 0;

	//Get number of cores
	res = stats_result_set_find(stats, isk_node_cpu_count, devid);
	if (!res || 
	    !isi_stats_key_error_value_ok(res->error)) {
		return -1;
	}
	num_cores = res->u.i32;

	//Get number of disks
	/*
	res = stats_result_set_find(stats, isk_node_disk_count, devid);
	if (!res ||
	    !isi_stats_key_error_value_ok(res->error)) {
		return -1;
	}
	num_disks = res->u.i32;
	*/

	log(TRACE, "%s: devid: %d cores: %d wper_cpu: %d", __func__,
	    devid, num_cores, wper_cpu);

	//Targeting 4 workers per cpu for now
	//Need to figure out how to factor in drives
	to_return = num_cores * wper_cpu;

	log(TRACE, "%s: returning %d workers", __func__, to_return);

	//Return a number
	return to_return;
}

static void
remove_excess_workers(struct context *ctx)//////根据ration,减去一些多余的workers
{
	int total_workers = 0, total_rationed_workers = 0, excess_workers = 0;
	int extra = 0;
	bool reached_min = false;
	void **ptr;
	struct wp_node *curr_node = NULL;

	if (!ctx->update_node_rations) {
		return;
	}

	PVEC_FOREACH(ptr, &ctx->wp_nodes) {
		curr_node = *ptr;
		total_workers += curr_node->max_workers;
		curr_node->rationed_workers = curr_node->max_workers;
	}

	//Cut workers based on the ration.
	total_rationed_workers = (ctx->ration > 0 && ctx->ration < 100) ?
	    (total_workers * (float) ctx->ration / 100) : total_workers;

	//Calculate the number of excess workers.
	//rationed_workers is always <= total_workers. We have to cut workers
	//if the ration is < the total, and we have to cut even more
	//workers if the ration is > max workers per cluster.
	if (total_workers > total_rationed_workers)
		excess_workers += total_workers - total_rationed_workers;
	if (total_rationed_workers > max_wper_cluster &&
	    max_wper_cluster != -1)
		excess_workers += total_rationed_workers - max_wper_cluster;

	//Bring all nodes down to max worker per node limit. If we cut workers
	//here, we can remove them from excess_workers count.
	if (max_wper_node != -1) {
		PVEC_FOREACH(ptr, &ctx->wp_nodes) {
			curr_node = *ptr;
			if (curr_node->rationed_workers > max_wper_node) {
				extra = curr_node->rationed_workers - 
				    max_wper_node;
				curr_node->rationed_workers -= extra;
				excess_workers -= extra;
			}
		}
	}

	//If we still have excess workers, remove one from each node in a
	//round-robin fashion. Do not let any nodes fall below having at least
	//one worker regardless of the limit.
	while (excess_workers > 0 && !reached_min) {
		reached_min = true;
		PVEC_FOREACH(ptr, &ctx->wp_nodes) {
			curr_node = *ptr;
			if (curr_node->rationed_workers > 1) {
				curr_node->rationed_workers--;
				excess_workers--;
			}
			if (!excess_workers)
				break;
			if (curr_node->rationed_workers != 1)
				reached_min = false;
		}
	}

	ctx->update_node_rations = false;
}

static void
update_node_max_workers(struct context *ctx, struct isi_error **error_out)////更新这个node上可以放的最多workers
{
	bool got_all_workers = true;
	int workers = 0;
	void **ptr = NULL;
	struct stats_result_set stats = {};
	struct uint16_set UINT16_SET_INIT_CLEAN(devids);
	struct ptr_vec node_set = PVEC_INITIALIZER;
	struct node_ids *current = NULL;
	struct wp_node *new_node = NULL;
	struct isi_error *error = NULL;

	if (!ctx->update_node_info) {
		return;
	}

	//isi_stats API call
	stats_result_set_init(&stats);
	stats_query_current(&stats, &ctx->isi_stats_keys, &devids,
	    ISI_STATS_TIMEOUT, sqf_degraded, &error);  //is sqf_degraded correct?

	UFAIL_POINT_CODE(force_stat_failure,
	    error = isi_system_error_new(EINVAL, "force_stat_failure "
		    "failpoint");
	);

	if (error) {
		log(ERROR, "%s: Failed to stats_query_current: %s", __func__,
		    isi_error_get_message(error));
		goto out;
	}

	get_up_node_ids(&node_set, &error);
	if (error) {
		log(ERROR, "Failed to get up node ids: %s",
		    isi_error_get_message(error));
		goto out;
	}

	//Delete all existing entries
	free_and_clean_pvec(&ctx->wp_nodes);

	//Iterate over responses and populate in-memory structure
	PVEC_FOREACH(ptr, &node_set) {
		current = *ptr;
		CALLOC_AND_ASSERT(new_node, struct wp_node);
		new_node->lnn = current->lnn;
		workers = get_workers(&stats, current->devid);

		UFAIL_POINT_CODE(fail_get_workers,
		    if (current->devid == RETURN_VALUE)
			    workers = -1;
		);

		if (workers <= 0) {
			workers = wper_cpu;
			got_all_workers = false;
			log(ERROR, "Failed to get workers for devid %d: lnn: "
			    "%d. Setting worker count to %d.", current->devid,
			    current->lnn, workers);
		} else {
			log(NOTICE, "%s: adding devid %d lnn %d with worker "
			    "count %d", __func__, current->devid,
			    new_node->lnn, workers);
		}
		if (set_wper_node != -1) {
			/* Override calculated values */
			new_node->max_workers = set_wper_node;
			new_node->rationed_workers = set_wper_node;
		} else {
			new_node->max_workers = workers;
			new_node->rationed_workers = workers;
		}
		pvec_append(&ctx->wp_nodes, new_node);
	}

	if (got_all_workers)
		ctx->update_node_info = false;

out:
	stats_result_set_clean(&stats);
	free_and_clean_pvec(&node_set);
	isi_error_handle(error, error_out);
}

static void
populate_node_info(struct context *ctx)
{
	void **ptr;
	struct wp_node *curr_node = NULL;
	char *path = NULL;
	int res = 0;
	int total_workers = 0;
	struct isi_error *error = NULL;

	log(TRACE, "%s: enter", __func__);

	//Update max workers each node can support
	update_node_max_workers(ctx, &error);
	if (error)
		goto out;

	//Update how many workers each node can support given the current
	//ration and/or max worker limits
	remove_excess_workers(ctx);

	//Write the rationed amount of workers to disk for daemon processing
	PVEC_FOREACH(ptr, &ctx->wp_nodes) {
		curr_node = *ptr;

		//Construct path and write to it
		asprintf(&path, "%s/%d", SIQ_WORKER_POOL_DIR, curr_node->lnn);
		log(DEBUG, "%s: writing rationed_workers %d to lnn %d", __func__,
		    curr_node->rationed_workers, curr_node->lnn);
		ASSERT(path);
		ASSERT(curr_node->rationed_workers > 0,
		    "rationed_workers == %d", curr_node->rationed_workers);
		res = write_lnn_file(path, curr_node->rationed_workers);
		total_workers += curr_node->rationed_workers;
		free(path);
		path = NULL;
	}
	num_nodes = pvec_size(&ctx->wp_nodes);

	write_total_workers(total_workers);

out:
	isi_error_free(error);
}

static void
reallocate_workers(struct context *ctx)
{
	void **ptr;
	struct job *job;
	struct wp_node *node = NULL;
	int total_workers = 0;
	int num_jobs = 0;
	int remainder = 0;
	bool under_split = false;
	bool no_nodes = true;
	char agg_node_mask[SIQ_MAX_NODES + 1];
	char all_node_mask[SIQ_MAX_NODES + 1];
	struct generic_msg GEN_MSG_INIT_CLEAN(msg);
	struct siq_ript_msg *ript = NULL;
	int i = 0;
	int wpn_per_job_limit = 0;
	int effective_per_job_ration = 0;

	//Iterate jobs and aggregrate the node masks of jobs to detect ignored
	//nodes
	init_node_mask(all_node_mask, 'F', sizeof(all_node_mask));
	PVEC_FOREACH(ptr, &ctx->jobs) {
		job = *ptr;
		for (i = 0; i < strlen(job->possible_nodes); i++) {
			if (job->possible_nodes[i] == '.') {
				all_node_mask[i] = '.';
				no_nodes = false;
			}
		}
	}

	//Calculate and cache total worker max per node from isi_stats data
	//Nodes will never change cpu counts, but can change drive counts.
	//Recalculate on group change, worker limit config settings changes and
	//ration change.
	populate_node_info(ctx);
	PVEC_FOREACH(ptr, &ctx->wp_nodes) {
		node = *ptr;
		//If no work can be done on a node, ignore it unless there are
		//no possible nodes - there are probably no jobs too
		if (all_node_mask[node->lnn - 1] == '.' || no_nodes) {
			total_workers += node->rationed_workers;
		}
	}

	//Calculate even split of workers
	num_jobs = pvec_size(&ctx->jobs);

	//The ration has already been applied in populate_node_info().
	ctx->total_rationed_workers = total_workers;
	if (ctx->total_rationed_workers <= 0) {
		log(NOTICE, "%s: %d total rationed workers", __func__,
		    ctx->total_rationed_workers);
	}

	ctx->per_job_ration = ctx->total_rationed_workers /
	    (num_jobs > 0 ? num_jobs : 1);
	ctx->remainder_workers = ctx->total_rationed_workers %
	    (num_jobs > 0 ? num_jobs : 1);

	if (ctx->per_job_ration < 1) {
		ctx->per_job_ration = 1;
		ctx->remainder_workers = 0;
		ctx->total_rationed_workers = num_jobs;
	} else if (ctx->per_job_ration >= max_wper_job && max_wper_job != -1) {
		ctx->per_job_ration = max_wper_job;
		ctx->remainder_workers = 0;
	}

	if (max_wpn_per_job != -1) {
		wpn_per_job_limit = max_wpn_per_job * num_nodes;
		if (ctx->per_job_ration >= wpn_per_job_limit) {
			ctx->per_job_ration = wpn_per_job_limit;
			ctx->remainder_workers = 0;
		}
	}

	log(DEBUG, "%s: total_workers: %d ration: %d%% "
	    "total_rationed_workers: %d num_jobs: %d per_job_ration: %d "
	    "remainder_workers: %d", __func__, total_workers, ctx->ration,
	    ctx->total_rationed_workers, num_jobs, ctx->per_job_ration,
	    ctx->remainder_workers);

	//Iterate jobs and aggregrate the node masks of jobs under split
	init_node_mask(agg_node_mask, '.', sizeof(agg_node_mask));
	PVEC_FOREACH(ptr, &ctx->jobs) {
		job = *ptr;

		if (job->requested_workers != 0 &&
		    job->requested_workers < ctx->per_job_ration) {
			effective_per_job_ration = job->requested_workers;
		} else {
			effective_per_job_ration = ctx->per_job_ration;  //////走这一路
		}
		if (job->num_workers < effective_per_job_ration) {//////当前这个job有的workers < job算出来的workers数量
			merge_and_flip_node_mask(agg_node_mask, 
			    job->possible_nodes, sizeof(agg_node_mask)); ///job->possible_nodes: '...... ---> 'agg_node_mask: 'FFFFFFFF'
			under_split = true;
		}
	}
		
	//Everything is over ration, so do not set any masks
	if (!under_split) {//////job->num_workers > effective_per_job_ration
		init_node_mask(agg_node_mask, '.', sizeof(agg_node_mask));///agg_node_mask: "......"
	}

	//Iterate jobs and apply aggregrate node mask for jobs over split,
	//clear mask for nodes under split
	PVEC_FOREACH(ptr, &ctx->jobs) {
		job = *ptr;

		if (job->requested_workers != 0 &&
		    job->requested_workers < ctx->per_job_ration) {
			log(TRACE, "%s: applying requested_workers %d instead "
			    "of per_job_ration %d", __func__,
			    job->requested_workers, ctx->per_job_ration);
			effective_per_job_ration = job->requested_workers;
		} else {
			effective_per_job_ration = ctx->per_job_ration;   //////走这一路
		}
		//Add 1 to per_job_ration to avoid fractional worker issues
		if (job->num_workers > (effective_per_job_ration + 1)) { /////需要减少workers
			//Too many workers, set limiting mask
			log(TRACE, "%s: job %s has too many workers (%d vs %d)",
			    __func__, job->name, job->num_workers,
			    effective_per_job_ration);
			memcpy(job->allowed_nodes, agg_node_mask,
			    strlen(agg_node_mask) + 1);
		} else {
			//Too few workers, clear mask     需要增加workers
			log(TRACE, "%s: job %s has too few workers (%d vs %d)",
			    __func__, job->name, job->num_workers,
			    effective_per_job_ration);
			init_node_mask(job->allowed_nodes, '.',
			    sizeof(job->allowed_nodes));
		}
	}

	//Iterate and send all worker messages. Distribute remainder workers
	//round-robin style.
	remainder = ctx->remainder_workers;
	msg.head.type = build_ript_msg_type(WORKER_MSG);
	ript = &msg.body.ript;
	ript_msg_init(ript);
	PVEC_FOREACH(ptr, &ctx->jobs) {/////遍历每一个jobs等价于遍历每一个coord
		job = *ptr;

		ript_msg_set_field_uint32(ript, RMF_MAX_ALLOWED_WORKERS, 1, ////每个job分到的ration
		    ctx->per_job_ration + (remainder-- > 0 ? 1:0));

		ript_msg_set_field_str(ript, RMF_ALLOWED_NODES, 1,
		    job->allowed_nodes);

		msg_send(job->socket, &msg);////给coord发消息: WORKER_MSG
	}
}

static void
reallocate(struct context *ctx)
{
	if (ctx->bwt_type == BWT_LIM_BW || ctx->bwt_type == BWT_LIM_TH)
		reallocate_bw_or_fps(ctx);
	else if (ctx->bwt_type == BWT_LIM_CPU)
		reallocate_cpu(ctx);
	else if (ctx->bwt_type == BWT_LIM_WORK)
		reallocate_workers(ctx);
	else
		ASSERT(!"unexpected index");
}

static void
adjust_cpu_ration(struct context *ctx, int actual_cpu_usage)
{
	int new_adjusted_ration;
	int num_workers = pvec_size(&ctx->workers);

	log(TRACE, "%s", __func__);

	if (ctx->ration == -1 || ctx->ration == 100)
		/* no limits */
		goto out;

	/* if nothing's running, reset adjusted ration */
	if (num_workers == 0) {
		ctx->adjusted_ration = ctx->ration;
		goto out;
	}

	/* if cpu usage on the node by workers is not meeting the limit, it
	 * may be that some are being throttled while others are unable to use
	 * their whole ration. in case this is happening, we artificially
	 * elevate the limit to try and give the throttled workers a little
	 * ration to make up for the workers that can't use their whole ration.
	 *
	 * on the other hand, if we're using too much CPU, we lower the
	 * adjusted limit, but don't bring it down any lower than the actual
	 * ration */

	if (abs(actual_cpu_usage - ctx->ration) < 2)
		/* dead band, no change */
		goto out;

	/* make adjustments proportional to the difference between the actual
	 * and target values */
	new_adjusted_ration = ctx->adjusted_ration * ctx->ration /
	    ((actual_cpu_usage > 0) ? actual_cpu_usage : 1);

	/* to stay within limits, no worker should ever need a ration higher
	 * than the total node ration, so adjusted ration is limited to
	 * num_workers * ration */
	if (new_adjusted_ration > num_workers * ctx->ration)
		new_adjusted_ration = num_workers * ctx->ration;

	/* adjusted ration should not need to go below the target ration
	 * (unless the workers are misbehaving) */
	if (new_adjusted_ration < ctx->ration)
		new_adjusted_ration = ctx->ration;

	log(DEBUG, "target: %d%% actual: %d%% adjusted: %d%%->%d%%",
	    ctx->ration, actual_cpu_usage, ctx->adjusted_ration,
	    new_adjusted_ration);

	ctx->adjusted_ration = new_adjusted_ration;
	reallocate_cpu(ctx);

out:
	return;
}

static void
conf_read(struct context *ctx)
{
	int n = 0;
	struct timeslot *ts = 0;
	struct bwt_limit_head *head = NULL;
	struct bwt_limit *rule = NULL;

	switch (ctx->bwt_type) {
		case BWT_LIM_BW:
			head = &siq_bc->root->bw;
			break;
		case BWT_LIM_TH:
			head = &siq_bc->root->th;
			break;
		case BWT_LIM_CPU:
			head = &siq_bc->root->cpu;
			break;
		case BWT_LIM_WORK:
			head = &siq_bc->root->work;
			break;
		default:
			ASSERT(false, "Invalid bwt_type");
	}
	
	SLIST_FOREACH(rule, head, next) {
		n++;
		if (rule->state != BWT_ST_ON) {
			log(DEBUG, "timeslot #%d is disabled, skipping", n);
			continue;
		}

		ts = calloc(1, sizeof(*ts));

		/* Try to read day */
		ts->days = rule->schedule->days;
		if (ts->days < 0)
			ts->days = 0x7f;

		/* Try to read time */
		ts->start = rule->schedule->start;
		if (ts->start >= 0) {
			ts->end = rule->schedule->end;
			/* round any end time between 11:55pm and 11:59pm to
			 * midnight */
			if (ts->end == 0 || ts->end >= 24 * 60 - 5)
				ts->end = 24 * 60;
		}

		if (ts->start < 0 || ts->end < 0) {
			ts->start = 0;
			ts->end = 24 * 60;
		}

		/* Try to read limit */
		ts->limit = rule->limit;

		/* No line is complete without a limit */
		if (ts->limit < -1)
			log(FATAL, "%s config entry %d is invalid",
			    ctx->type, n);

		log(DEBUG, "read timeslot: {%x, %d, %d, %d}",
		    ts->days, ts->start, ts->end, ts->limit);

		pvec_append(&ctx->timeslots, (void *)ts);
		ts = NULL;
	}
}

static int
conf_limit(struct ptr_vec *timeslots)
{
	struct timeslot *match = NULL, *cur = NULL;
	struct tm *tm;
	time_t t;
	int mins, day;
	void **ptr;

	t = time(0);
	tm = localtime(&t);
	mins = tm->tm_hour * 60 + tm->tm_min;
	day = 1 << tm->tm_wday;

	/* Select the limit for the current time, if rules overlap, select the
	 * most restrictive rule. */
	PVEC_FOREACH(ptr, timeslots) {
		cur = *ptr;
		if (cur->days & day && mins >= cur->start && mins < cur->end) {
			if (!match) {
				match = cur;
			} else if (match->limit > cur->limit) {
				match = cur;
			}
		}
	}

	return match ? match->limit : -1;
}

/* Calls by sighup on MCP signal are distinguished by reload argument */
static void
conf_check(struct context *ctx, bool reload)
{
	struct timeslot *cur = NULL;
	int ration;
	void **ptr;
	struct isi_error *error = NULL;
	log(TRACE, "conf_check");

	if (reload) {
		if (siq_bc)
			bwt_gc_conf_free(siq_bc);

		if ((siq_bc = bwt_gc_conf_load(&error)) == NULL)
			log(FATAL, "Failed to load bwt configuration");
		bwt_gc_conf_close(siq_bc);

		PVEC_FOREACH(ptr, &ctx->timeslots) {
			cur = *ptr;
			free(cur);
		}
		pvec_clean(&ctx->timeslots);

		conf_read(ctx);
	}

	ration = conf_limit(&(ctx->timeslots));

	if (ration == ctx->ration) {
		/* no changes to send */
		goto out;
	}

	if (ctx == &cpu_ctx || ctx == &work_ctx) {
		if (ration > 100)
			ration = 100;
		if (ration < -1)
			ration = -1;

		/*  reset adjusted ration */
		ctx->adjusted_ration = ration;

		/* Recalculate per-node worker max with new ration */
		if (ctx == &work_ctx)
			ctx->update_node_rations = true;
	}

	ctx->pool += ration - ctx->ration;
	ctx->ration = ration;

	log(INFO, "%s limit %d %s", ctx->type, ctx->ration, ctx->unit);
	timer_cb((void *)1);
	reallocate(ctx);

out:
	isi_error_free(error);
	return;
}

/*   ____      _ _ _                _        
 *  / ___|__ _| | | |__   __ _  ___| | _____ 
 * | |   / _` | | | '_ \ / _` |/ __| |/ / __|
 * | |__| (_| | | | |_) | (_| | (__|   <\__ \
 *  \____\__,_|_|_|_.__/ \__,_|\___|_|\_\___/
 */

static int
bandwidth_init_cb(struct generic_msg *m, void *sock)////master bandwidth node收到coord连接之后的回调函数
{
	struct job *job;

	log(DEBUG, "bandwidth_init_cb for policy %s with %d workers", 
	    m->body.bandwidth_init.policy_name,
	    m->body.bandwidth_init.num_workers);
       
	job = calloc(1, sizeof *job);
	pvec_insert(&bw_ctx.jobs, 0, (void *)job); //////新来的job加到bw_ctx.jobs中去

	job->ctx = &bw_ctx;
	strncpy(job->name, m->body.bandwidth_init.policy_name, MAXNAMELEN);
	job->num_workers = m->body.bandwidth_init.num_workers; /////初始时,coord connect master bandwidth，传的num_workers为0
	job->priority = m->body.bandwidth_init.priority;    ///0
	job->socket = (intptr_t)sock;
	bw_ctx.worker_total += m->body.bandwidth_init.num_workers;

	migr_register_callback(job->socket, BANDWIDTH_STAT_MSG,
	    bandwidth_stat_cb);
	migr_register_callback(job->socket, DISCON_MSG,
	    bw_or_fps_disconnect_cb);

	migr_set_fd(job->socket, job);
	reallocate(&bw_ctx);

	return 0;
}

static int
throttle_init_cb(struct generic_msg *m, void *sock)
{
	struct job *job;
       
	log(DEBUG, "throttle_init_cb for policy %s with %d workers", 
	    m->body.throttle_init.policy_name,
	    m->body.throttle_init.num_workers);

	job = calloc(1, sizeof *job);
	pvec_insert(&th_ctx.jobs, 0, (void *)job);

	job->ctx = &th_ctx;
	strncpy(job->name, m->body.throttle_init.policy_name, MAXNAMELEN);
	job->num_workers = m->body.throttle_init.num_workers;
	job->priority = m->body.throttle_init.priority;
	job->socket = (intptr_t)sock;
	th_ctx.worker_total += m->body.throttle_init.num_workers;

	migr_register_callback(job->socket, THROTTLE_STAT_MSG,
	    throttle_stat_cb);
	migr_register_callback(job->socket, DISCON_MSG,
	    bw_or_fps_disconnect_cb);

	migr_set_fd(job->socket, job);
	reallocate(&th_ctx);

	return 0;
}

/* find/add the given ip address to the list of cpu throttle hosts,
 * and update is usage percentage */
static struct host *
update_node_cpu_usage(char *ip, int usage_pct, struct isi_error **error_out)
{
	struct host *host = NULL;
	struct isi_error *error = NULL;

	if (ip == NULL) {
		error = isi_system_error_new(EINVAL, "Unable to update node "
		    "usage for NULL ip");
		goto out;
	}

	host = find_or_add_host(&cpu_ctx, ip);
	host->usage = usage_pct;

out:
	isi_error_handle(error, error_out);
	return host;
}

static void
update_master_cpu_usage(int usage_pct)
{
	struct isi_error *error = NULL;

	if (g_ctx.ip == NULL) {
		get_ip_address(&g_ctx.ip, g_ctx.devid, true, &error);
		if (error || g_ctx.ip == NULL)
			goto out;
	}

	update_node_cpu_usage(g_ctx.ip, usage_pct, &error);

out:
	isi_error_free(error);
}

/* Add a node to our list of cpu throttle hosts and prepare to
 * receive node cpu usage stats from it. */
static int
cpu_usage_init_cb(struct generic_msg *m, void *sock)
{
	struct host *host;
	struct siq_ript_msg *ript = &m->body.ript;
	char *host_name = NULL;
	struct isi_error *error = NULL;

	RIPT_MSG_GET_FIELD(str, ript, RMF_CPU_INIT_NAME, 1, &host_name, &error,
	    out);

	/* Add the node to our list of cpu throttle hosts and set
	 * its usage_pct to 0. This function also initializes
	 * host->name and host->usage.*/
	host = update_node_cpu_usage(host_name, 0, &error);
	if (error)
		goto out;

	host->socket = (intptr_t)sock;

	migr_register_callback(host->socket,
	    build_ript_msg_type(CPU_NODE_USAGE_MSG), cpu_node_usage_cb);
	migr_register_callback(host->socket, DISCON_MSG,
	    cpu_usage_disconnect_cb);

	migr_set_fd((intptr_t)sock, host);

out:
	if (error) {
		log(ERROR, "Failed to initialize cpu usage callback: %s",
		    isi_error_get_message(error));
		isi_error_free(error);
	}

	return 0;
}

static int
cpu_throttle_init_cb(struct generic_msg *m, void *sock)/////pworker第一次连接bandwidth时触发,bandwidth负责创建这个worker
{
	struct worker *worker;

	worker = calloc(1, sizeof *worker);
	worker->socket = (intptr_t)sock;   //////bandwidth和pworker(cpu_throttle)通信的套接字
	worker->type = m->body.cpu_throttle_init.worker_type;////pworker/sworker

	switch (worker->type) {
	case SIQ_WT_PWORKER:
		log(DEBUG, "%s: pworker", __func__);
		cpu_ctx.pworker_total++;
		break;

	case SIQ_WT_SWORKER:
		log(DEBUG, "%s: sworker", __func__);
		cpu_ctx.sworker_total++;
		break;

	case SIQ_WT_P_HASHER:
		log(DEBUG, "%s: p_hasher", __func__);
		cpu_ctx.p_hasher_total++;
		break;

	case SIQ_WT_S_HASHER:
		log(DEBUG, "%s: s_hasher", __func__);
		cpu_ctx.s_hasher_total++;
		break;

	case SIQ_WT_UNKNOWN:
	default:
		ASSERT(!"unexpected worker type!");
	}

	pvec_insert(&cpu_ctx.workers, 0, (void *)worker);

	migr_register_callback(worker->socket, CPU_THROTTLE_STAT_MSG, //////worker执行send_cpu_stat_update(),会发送CPU_THROTTLE_STAT_MSG消息(发送cpu_time_elapsed,time_elapsed)
	    cpu_throttle_stat_cb);
	migr_register_callback(worker->socket, DISCON_MSG, cpu_disconnect_cb);

	migr_set_fd(worker->socket, worker);
	reallocate(&cpu_ctx);

	return 0;
}

static int
worker_init_cb(struct generic_msg *m, void *sock)
{
	struct job *job;
	struct siq_ript_msg *ript = &m->body.ript;
	char *policy_name = NULL, *node_mask = NULL;
	int ret = -1;
	struct isi_error *error = NULL;

	job = calloc(1, sizeof *job); /////一个job相当于一个coord,里面包含了这个coord中所有workers
	pvec_insert(&work_ctx.jobs, 0, (void *)job);

	job->ctx = &work_ctx;
	job->socket = (intptr_t)sock;

	RIPT_MSG_GET_FIELD(str, ript, RMF_POLICY_NAME, 1, &policy_name,
	    &error, out)
	RIPT_MSG_GET_FIELD(str, ript, RMF_NODE_MASK, 1, &node_mask,
	    &error, out)

	strncpy(job->name, policy_name, MAXNAMELEN);
	ASSERT(strlen(node_mask) <= SIQ_MAX_NODES);
	strcpy(job->possible_nodes, node_mask);//////初始都是"......"
	strcpy(job->allowed_nodes, node_mask);///初始都是"...."

	log(DEBUG, "%s: policy %s", __func__, policy_name);
	log(TRACE, "%s: node mask: %s", __func__, node_mask);

	migr_register_callback(job->socket,
	    build_ript_msg_type(WORKER_STAT_MSG), worker_stat_cb);
	migr_register_callback(job->socket,
	    build_ript_msg_type(WORKER_NODE_UPDATE_MSG),
	    worker_node_update_cb);
	migr_register_callback(job->socket, DISCON_MSG,
	    worker_disconnect_cb);

	migr_set_fd(job->socket, job);
	reallocate(&work_ctx);

	UFAIL_POINT_CODE(dump_ript_msg,
	    dump_status(NULL);
	);

	ret = 0;

out:
	if (error) {
		log(FATAL, "%s: Error parsing ript msg: %s", __func__,
		    isi_error_get_message(error));
	}

	return ret;
}

static struct host *
find_or_add_host(struct context *ctx, char *name)
{
	struct host *host = NULL;
	void **ptr;
	char path[MAXPATHLEN + 1];
	char key[MAX_KEY_LEN + 1];
	FILE *keyfile = NULL;

	log(TRACE, "find_or_add_host");

	PVEC_FOREACH(ptr, &ctx->hosts) {
		host = *ptr;
		if (strcmp(host->name, name) == 0)
			break;
		host = NULL;
	}

	if (!host) {
		host = calloc(1, sizeof(struct host));
		strcpy(host->name, name);
		pvec_append(&ctx->hosts, host);

		/* add new host to key file - if it doesn't already exist */
		snprintf(path, MAXPATHLEN + 1, "%s/%s", SIQ_BWT_WORKING_DIR,
		    key_file_name(ctx));
		if ((keyfile = fopen(path, "a+"))) {
			fseek(keyfile, 0, SEEK_SET);
			while (fgets(key, MAX_KEY_LEN, keyfile)) {
				if (!strncmp(name, key, strlen(name)))
					goto out;
			}
			log(NOTICE, "added host %s to %s", name,
			    key_file_name(ctx));
			fprintf(keyfile, "%s\n", name);
		} else
			log(ERROR, "error opening key file %s", path);
	}

out:
	if (keyfile != NULL) {
		fclose(keyfile);
	}
	return host;
}

static int
bandwidth_stat_cb(struct generic_msg *m, void *ctx)
{
	struct job *job = ctx;
	struct host *host;

	if (strcmp(m->body.bandwidth_stat.name, "job_total") == 0) {
		/* job total stat message */
		job->usage = m->body.bandwidth_stat.bytes_sent;
		log(TRACE, "bandwidth_stat_cb: job %s reported %llu bytes sent",
		    job->name, job->usage);
	} else {
		/* locate the host (create if necessary) and adjust stat */
		host = find_or_add_host(job->ctx, m->body.bandwidth_stat.name);
		host->usage += m->body.bandwidth_stat.bytes_sent;
	}

	return 0;
}

static int
throttle_stat_cb(struct generic_msg *m, void *ctx)
{
	struct job *job = ctx;
	struct host *host;

	if (strcmp(m->body.throttle_stat.name, "job_total") == 0) {
		/* job total stat message */
		job->usage = m->body.throttle_stat.files_sent;
		log(TRACE, "throttle_stat_cb: job %s reported %llu files sent",
		    job->name, job->usage);
	} else {
		/* locate the host (create if necessary) and adjust stat */
		host = find_or_add_host(job->ctx, m->body.throttle_stat.name);
		host->usage += m->body.throttle_stat.files_sent;
	}

	return 0;
}

/* worker cpu stats -> node's bandwidth daemon */
static int
cpu_throttle_stat_cb(struct generic_msg *m, void *ctx)
{
	struct worker *w = ctx;

	w->cpu_time = m->body.cpu_throttle_stat.cpu_time;
	w->time = m->body.cpu_throttle_stat.time;

	log(TRACE, "%s %llu%% (%llu/%llu)", __func__,
	    w->cpu_time * 100 / w->time, w->cpu_time, w->time);
	return 0;
}

/* node's total worker cpu usage -> master bandwidth daemon.
 * ctx is the host that sent the message, we just need to update that
 * host's cpu usage percentage. The host is a member of cpu_ctx.hosts
 * and was added when the node sent the CPU_USAGE_INIT message. */
static int
worker_stat_cb(struct generic_msg *m, void *ctx)
{
	struct job *job = ctx;
	struct siq_ript_msg *ript = &m->body.ript;
	int ret = -1;
	uint32_t workers_used = -1;
	int requested_workers = 0;
	struct isi_error *error = NULL;

	RIPT_MSG_GET_FIELD(uint32, ript, RMF_WORKERS_USED, 1, &workers_used, /////g_ctx.workers->count 当前这个job正在用的workers数量
	    &error, out)
	RIPT_MSG_GET_FIELD_ENOENT(int32, ript, RMF_REQUESTED_WORKERS, 1,
	    &requested_workers, &error, out);

	job->num_workers = workers_used;
	job->requested_workers = requested_workers;/////每个node上最大的workers数量 * node个数
	log(TRACE, "%s: job %s reported %d workers used "
	    "%d requested workers", __func__, job->name, job->num_workers,
	    job->requested_workers);

	ret = 0;

out:
	if (error) {
		log(FATAL, "%s: Error parsing ript msg: %s", __func__,
		    isi_error_get_message(error));
	}

	return ret;
}

static int
cpu_node_usage_cb(struct generic_msg *m, void *ctx)
{
	struct host *host = ctx;
	struct siq_ript_msg *ript = &m->body.ript;
	char *host_name = NULL; 
	uint32_t usage = 0;
	struct isi_error *error = NULL; 

	RIPT_MSG_GET_FIELD(str, ript, RMF_CPU_NODE_USAGE_NAME, 1, &host_name,
	    &error, out);
	RIPT_MSG_GET_FIELD(uint32, ript, RMF_CPU_NODE_USAGE_PCT, 1, &usage,
	    &error, out);
	ASSERT(!strcmp(host->name, host_name));

	host->usage = usage;///////这个node上的total (cpu_time/time)

out:
	if (error) {
		log(ERROR, "Failed to parse node cpu usage: %s",
		    isi_error_get_message(error));
		isi_error_free(error);
	}
	return 0;
}

static int
worker_node_update_cb(struct generic_msg *m, void *ctx)
{
	struct job *job = ctx;
	struct siq_ript_msg *ript = &m->body.ript;
	int ret = -1;
	char *node_mask = NULL;
	struct isi_error *error = NULL;

	RIPT_MSG_GET_FIELD(str, ript, RMF_NODE_MASK, 1, &node_mask,/////这个job中当前还活着的worker
	    &error, out)

	strcpy(job->possible_nodes, node_mask);
	strcpy(job->allowed_nodes, node_mask);

	log(DEBUG, "%s: policy: %s node_mask: %s", __func__, job->name,
	    node_mask);

	reallocate(&work_ctx);

	ret = 0;

out:
	if (error) {
		log(FATAL, "%s: Error parsing ript msg: %s", __func__,
		    isi_error_get_message(error));
	}

	return ret;
}


static int
uninit_disconnect_cb(struct generic_msg *m, void *ctx)
{
	/* client who never initialized; ctx is fd */
	log(TRACE, "disconnect from uninitialized client");
	migr_rm_fd((intptr_t)ctx);
	close((intptr_t)ctx);
	return 0;
}

static int
bw_or_fps_disconnect_cb(struct generic_msg *m, void *ctx)
{
	struct job *job;
	struct context *job_ctx;
	int index = 0;

	log(TRACE, "%s", __func__);

	job = ctx;
	job->ctx->pool += job->ration;
	close(job->socket);
	migr_rm_fd(m->body.discon.fd);
	job->ctx->worker_total -= job->num_workers;

	log(TRACE, "disconnect from job %s", job->name);

	/* remove job from the list */
	ASSERT(pvec_find(&job->ctx->jobs, job, &index));
	pvec_remove(&job->ctx->jobs, index);
	job_ctx = job->ctx;
	free(job);

	reallocate(job_ctx);

	return 0;
}

static int
cpu_disconnect_cb(struct generic_msg *m, void *ctx)
{
	struct worker *worker = ctx;
	int index = 0;

	switch (worker->type) {
	case SIQ_WT_PWORKER:
		log(DEBUG, "%s pworker", __func__);
		cpu_ctx.pworker_total--;
		ASSERT(cpu_ctx.pworker_total >= 0);
		break;

	case SIQ_WT_SWORKER:
		log(DEBUG, "%s sworker", __func__);
		cpu_ctx.sworker_total--;
		ASSERT(cpu_ctx.sworker_total >= 0);
		break;

	case SIQ_WT_P_HASHER:
		log(DEBUG, "%s p_hasher", __func__);
		cpu_ctx.p_hasher_total--;
		ASSERT(cpu_ctx.p_hasher_total >= 0);
		break;

	case SIQ_WT_S_HASHER:
		log(DEBUG, "%s s_hasher", __func__);
		cpu_ctx.s_hasher_total--;
		ASSERT(cpu_ctx.s_hasher_total >= 0);
		break;

	default:
		ASSERT(!"unexpected worker type!");
	}

	close(worker->socket);
	migr_rm_fd(m->body.discon.fd);

	ASSERT(pvec_find(&cpu_ctx.workers, worker, &index));
	pvec_remove(&cpu_ctx.workers, index);
	free(worker);

	reallocate(&cpu_ctx);

	return 0;
}

static void
exit_if_upgrade_committed(uint32_t handshake_result)
{
	struct version_status *old_ver_stat = &g_ctx.ver_stat;
	struct version_status new_ver_stat;

	if (!old_ver_stat->local_node_upgraded ||
	    handshake_result != SHAKE_PEER_REJ)
		goto out;

	get_version_status(true, &new_ver_stat);

	ASSERT(old_ver_stat->committed_version <
	    new_ver_stat.committed_version);

	log(NOTICE, "Upgrade committed (old version: 0x%x, new version: "
	    "0x%x), restarting", old_ver_stat->committed_version,
	    new_ver_stat.committed_version);

	/* Exit so all intra-cluster bw connections use new version when
	 * restarted. */
	exit(EXIT_SUCCESS);

out:
	return;
}

/* Master loses connection to non-master */
static int
worker_disconnect_cb(struct generic_msg *m, void *ctx)
{
	struct job *job;
	struct context *job_ctx;
	int index = 0;

	log(TRACE, "%s", __func__);
	//TODO: Double check this math
	job = ctx;
	job->ctx->pool += job->ration;
	close(job->socket);
	migr_rm_fd(m->body.discon.fd);

	log(TRACE, "disconnect from job %s", job->name);

	/* remove job from the list */
	ASSERT(pvec_find(&job->ctx->jobs, job, &index));
	pvec_remove(&job->ctx->jobs, index);
	job_ctx = job->ctx;
	free(job);

	reallocate(job_ctx);

	return 0;
}

static int
cpu_usage_disconnect_cb(struct generic_msg *m, void *ctx)
{
	struct host *host = ctx;

	close(host->socket);
	migr_rm_fd(m->body.discon.fd);

	return 0;
}

/* Non-master loses connection to master */
static int
master_disconnect_cb(struct generic_msg *m, void *ctx)
{
	ASSERT(g_ctx.master_fd != -1);

	close(g_ctx.master_fd);
	migr_rm_fd(g_ctx.master_fd);
	g_ctx.master_fd = -1;

	return 0;
}

static int
bw_fps_connect_cb(struct generic_msg *m, void *ctx)//////这里面的回调函数都是master node处理的
{
	int new_fd = *(int *)ctx;
	uint32_t max_ver = g_ctx.ver_stat.committed_version, prot_ver;

	log(TRACE, "%s", __func__);

	if (msg_handshake(new_fd, max_ver, &prot_ver, POL_MATCH_OR_FAIL)) {
		log(NOTICE, "handshake failed on connect: 0x%x", prot_ver);
		close(new_fd);
		exit_if_upgrade_committed(prot_ver);
		goto error;
	}

	log_version_ufp(prot_ver, SIQ_BANDWIDTH, __func__);

	migr_add_fd(new_fd, (void *)(intptr_t)new_fd, "coord");
	migr_register_callback(new_fd, BANDWIDTH_INIT_MSG, bandwidth_init_cb);///////coord连接master bandwidth
	migr_register_callback(new_fd, THROTTLE_INIT_MSG, throttle_init_cb);//////跳过,不考虑
	migr_register_callback(new_fd, build_ript_msg_type(WORKER_INIT_MSG),
	    worker_init_cb); /////不考虑worker?
	migr_register_callback(new_fd, build_ript_msg_type(CPU_USAGE_INIT_MSG),/////非master node连接master node
	    cpu_usage_init_cb);
	migr_register_callback(new_fd, DISCON_MSG, uninit_disconnect_cb);

	return 0;

error:
	return -1;
}

static int
cpu_connect_cb(struct generic_msg *m, void *ctx)/////每一个bandwidth都会注册这个函数:等待cpu_throttle的连接
{
	int new_fd = *(int *)ctx;
	uint32_t max_ver = g_ctx.ver_stat.committed_version, prot_ver;

	log(TRACE, "%s", __func__);

	if (msg_handshake(new_fd, max_ver, &prot_ver, POL_MATCH_OR_FAIL)) {
		log(NOTICE, "handshake failed on connect: 0x%x", prot_ver);
		close(new_fd);
		exit_if_upgrade_committed(prot_ver);
		goto error;
	}

	log_version_ufp(prot_ver, SIQ_BANDWIDTH, __func__);

	migr_add_fd(new_fd, (void *)(intptr_t)new_fd, "worker");
	migr_register_callback(new_fd, CPU_THROTTLE_INIT_MSG,
	    cpu_throttle_init_cb);
	migr_register_callback(new_fd, DISCON_MSG, uninit_disconnect_cb);

	return 0;

error:
	return -1;
}

static void
connect_to_master(void)
{
	int n_nodes = -1, i;
	char **ip_list = NULL;
	uint32_t ver = 0;
	struct generic_msg GEN_MSG_INIT_CLEAN(msg);
	struct siq_ript_msg *ript = NULL;
	struct isi_error *error = NULL;
	struct version_status new_ver_stat;

	/* Only connect to the master if this node can determine its ip */
	if (g_ctx.ip == NULL) {
		get_ip_address(&g_ctx.ip, g_ctx.devid, true, &error);

		UFAIL_POINT_CODE(dont_get_ip,
		    log(NOTICE, "dont_get_ip failpoint enabled.");
		    g_ctx.ip = NULL;
		);

		if (error || g_ctx.ip == NULL)
			goto out;
	}

	n_nodes = get_local_cluster_info(NULL, &ip_list, NULL, NULL, 0, NULL, 
	    true, true, NULL, &error);
	if (error || n_nodes <= 0)
		goto out;

	for (i = 0; ip_list[i]; i++) {
		if ((g_ctx.master_fd = connect_to(ip_list[i],
		    siq_gc->root->bwt->port, POL_MATCH_OR_FAIL,
		    g_ctx.ver_stat.committed_version, &ver, NULL, false,
		    &error)) != -1) {
			log(NOTICE, "Connected to master bandwidth host at "
			    "%s", ip_list[i]);
			log_version_ufp(ver, SIQ_BANDWIDTH, __func__);
			break;
		}

		if (error) {
			isi_error_free(error);
			error = NULL;
		}
	}

	if (g_ctx.master_fd == -1) {
		if (g_ctx.ver_stat.local_node_upgraded) {
			get_version_status(true, &new_ver_stat);

			if (g_ctx.ver_stat.committed_version <
			    new_ver_stat.committed_version) {

				log(NOTICE, "Upgrade committed (old version: "
				    "0x%x, new version: 0x%x), restarting",
				    g_ctx.ver_stat.committed_version,
				    new_ver_stat.committed_version);

				/* Exit so all intra-cluster bw connections
				 * use new version when restarted. */
				exit(EXIT_SUCCESS);
			}
		}
		goto out;
	}

	migr_add_fd(g_ctx.master_fd, &g_ctx, "master_bw");
	migr_register_callback(g_ctx.master_fd, DISCON_MSG,
	    master_disconnect_cb);

	msg.head.type = build_ript_msg_type(CPU_USAGE_INIT_MSG);
	ript = &msg.body.ript;
	ript_msg_init(ript);

	ript_msg_set_field_str(ript, RMF_CPU_INIT_NAME, 1, g_ctx.ip);
	msg_send(g_ctx.master_fd, &msg);

out:
	if (ip_list)
		free_ip_list(ip_list);
	isi_error_free(error);
}

static void
send_cpu_usage(int master_fd, float cpu_usage_pct)
{
	struct generic_msg GEN_MSG_INIT_CLEAN(msg);
	struct siq_ript_msg *ript = NULL;

	if (master_fd == -1)
		return;

	msg.head.type = build_ript_msg_type(CPU_NODE_USAGE_MSG);
	ript = &msg.body.ript;
	ript_msg_init(ript);

	ript_msg_set_field_uint32(ript, RMF_CPU_NODE_USAGE_PCT, 1,
	    (uint32_t)cpu_usage_pct);
	ript_msg_set_field_str(ript, RMF_CPU_NODE_USAGE_NAME, 1, g_ctx.ip);
	msg_send(master_fd, &msg);//////当前node把自己所有的worker的cpu使用率的和发送到master node
}

static void
log_bw_fps(struct context *ctx, bool force)
{
	void **ptr;
	struct host *host;
	uint64_t total = 0;
	int used = 0;
	int num_hosts = pvec_size(&ctx->hosts);

	PVEC_FOREACH(ptr, &ctx->hosts) {
		host = *ptr;
		total += host->usage;
	}

	UFAIL_POINT_CODE(no_hosts,
	    num_hosts = 0;
	    total = 0;
	);

	/* cpu total should be the average of all nodes, and all nodes have
	 * been assigned their entire ration */
	if (!strcmp(ctx->type, "cpu") && num_hosts > 0) {
		total /= num_hosts;
		used = ctx->ration;
	} else {
		used = MAX(ctx->ration - ctx->pool, 0);
	}

	if (!force && total == 0)
		goto out;

	rotate_log(ctx);
	if (!ctx->log && !open_log(ctx)) {
		log(ERROR, "Failed to open %s log", ctx->type);
		goto out;
	}

	/* log file data columns are as follows:
	 * 
	 * timestamp ration used total [host1] ... [hostN]
	 * 
	 * timestamp: output of time(0)
	 * 
	 * ration: total allowed ration at this time (kilobits/second)
	 * 
	 * used: amount of ration that is assigned to jobs (usually all if any
	 *     jobs are running)
	 *     
	 * total: total files or bytes sent in last 10 seconds for all jobs
	 *     (bytes/10 seconds)
	 *
	 * host: files/bytes sent in last 10 seconds by host N (see
	 *     corresponding key file)
	 */
	if (ctx->ration >= 0)
		fprintf(ctx->log, "%ld %d %d %llu", time(0),
		    ctx->ration, used, total);
	else
		fprintf(ctx->log, "%ld %d %d %llu", time(0), 0, 0, total);

	PVEC_FOREACH(ptr, &ctx->hosts) {
		host = *ptr;
		fprintf(ctx->log, " %llu", host->usage);
		host->usage = 0;
	}

	fprintf(ctx->log, "\n");
	fflush(ctx->log);

out:
	return;
}

static void
log_workers(struct context *ctx, bool force)
{
	void **ptr;
	struct job *job = NULL;
	int total_workers = 0;

	PVEC_FOREACH(ptr, &ctx->jobs) {
		job = *ptr;
		total_workers += job->num_workers;
	}

	if (!force && total_workers == 0)
		goto out;

	rotate_log(ctx);
	if (!ctx->log && !open_log(ctx)) {
		log(ERROR, "Failed to open %s log", ctx->type);
		goto out;
	}

	/* log file data columns are as follows:
	 *
	 * timestamp ration pool total
	 *
	 * timestamp: output of time(0)
	 * 
	 * ration: total allowed workers at this time
	 *
	 * pool: always equals 0 (not used for worker pools)
	 *
	 * total: total number of workers in use
	 */
	fprintf(ctx->log, "%ld %d %d %d\n", time(0), ctx->total_rationed_workers,
	    0, total_workers);
	fflush(ctx->log);

out:
	return;
}

static int
timer_cb(void *ctx)
{
	float cpu_usage_pct = 0.0;
	void **ptr;
	struct worker *w;
	static time_t next_min = 0;
	time_t curr_time;

	log(TRACE, "timer_cb");

	/* Check for a rule change at the first timer_cb of a new minute.
	 * The clock may have been rolled back during testing or cluster time
	 * syncing, so also check if the calculated next minute is more than a
	 * minute away */
	curr_time = time(NULL);
	if (curr_time > next_min || (next_min - curr_time) > 60) {  ///////隔一段时间回去重新读配置
		if (g_ctx.master) {
			conf_check(&bw_ctx, false);////做reallocate_bw_of_fps()
			conf_check(&th_ctx, false);
			conf_check(&work_ctx, false);
		}
		conf_check(&cpu_ctx, false);///做reallocate_cpu()
		next_min = curr_time + 60 - (curr_time % 60);
	}

	/* total CPU usage */
	PVEC_FOREACH(ptr, &cpu_ctx.workers) {
		w = *ptr;
		if (w->time > 0.0) {
			cpu_usage_pct += (float)w->cpu_time * 100.0 /
			    (float)w->time;
		}
	}

	if (g_ctx.master) {
		if (work_ctx.update_node_info || work_ctx.update_node_rations)
			reallocate(&work_ctx);
		log_bw_fps(&bw_ctx, ctx != 0);
		log_bw_fps(&th_ctx, ctx != 0);
		log_workers(&work_ctx, ctx != 0);

		/* The master must update its own cpu usage in its list of
		 * cpu throttle hosts */
		update_master_cpu_usage(cpu_usage_pct);///master bandwidth更新自己的cpu_usage
		log_bw_fps(&cpu_ctx, ctx != 0);
	} else{
		/* Report node cpu usage to the master */
		if (g_ctx.master_fd == -1)
			connect_to_master();
		send_cpu_usage(g_ctx.master_fd, cpu_usage_pct);////发送自己的cpu_usage给master bandwidth
	}

	log(DEBUG, "total node worker cpu usage: %d%%", (int)cpu_usage_pct);
	adjust_cpu_ration(&cpu_ctx, cpu_usage_pct);

	if (!ctx)
		migr_register_timeout(&timeout, timer_cb, NULL);

	return 0;
}

static int
group_cb(void *ctx)
{
	int lowest, bw_fps_listen_sock;
	bool was_master;

	log(TRACE, "group_cb");

	if (group_retry_tout) {
		migr_cancel_timeout(group_retry_tout);
		group_retry_tout = NULL;
	}

	if ((lowest = lowest_node()) == 0) {
		if (g_ctx.master) {
			/* Restart if master encounters an error */
			siq_nanosleep(5, 0);
			exit(EXIT_SUCCESS);
		} else {
			/* Retry in 10 seconds if not master */
			group_retry_tout = migr_register_timeout(&timeout,
			    group_cb, NULL);
			goto out;
		}
	}

	was_master = g_ctx.master;
	g_ctx.master = (g_ctx.devid == lowest);
	if (siq_gc->root->bwt->force_lnn != -1) {
		g_ctx.master = (g_ctx.devid == siq_gc->root->bwt->force_lnn);
	} else {
		g_ctx.master = (g_ctx.devid == lowest);
	}

	if (was_master && !g_ctx.master) {
		/* no longer master */
		log(NOTICE, "no longer master node, restarting to clean up");
		exit(EXIT_SUCCESS);
	}

	if (g_ctx.master) {
		//Update worker pool info
		work_ctx.update_node_info = true;
		work_ctx.update_node_rations = true;
	}

	if (g_ctx.master && !was_master) {
		log(NOTICE, "new master node, this daemon will service all "
		    "bandwidth and files per second requests");

		conf_check(&bw_ctx, true);
		conf_check(&th_ctx, true);
		conf_check(&work_ctx, true);
		reload_global_conf(false);

		//////只有master才会在3148端口上监听,接收其它node的cpu信息
		if ((bw_fps_listen_sock = 
		    listen_on(siq_gc->root->bwt->port, true)) < 0)
			log(FATAL, "failed to create socket");
		migr_add_listen_fd(bw_fps_listen_sock, NULL, "listen");
		migr_register_callback(bw_fps_listen_sock, CONN_MSG,
		    bw_fps_connect_cb);
	}

	/* all nodes handle cpu throttling */
	conf_check(&cpu_ctx, true);

out:
	return 0;
}

static void
reload_global_conf(bool load_only)
{
	struct isi_error *error = NULL;
	int prev_wper_node, prev_wper_cluster, prev_wper_job;
	int prev_set_wper_node, prev_wper_cpu, prev_wpn_per_job;
	bool workers_changed = false;

	/* This does a NULL check */
	siq_gc_conf_free(siq_gc);

	siq_gc = siq_gc_conf_load(&error);
	if (error)
		log(FATAL, "Failed to load global config: %s",
		    isi_error_get_message(error));
	siq_gc_conf_close(siq_gc);

	if (load_only) {
		max_wper_node = siq_gc->root->bwt->max_workers_per_node;
		max_wper_cluster = siq_gc->root->bwt->max_workers_per_cluster;
		max_wper_job = siq_gc->root->coordinator->max_wperpolicy;
		set_wper_node = siq_gc->root->bwt->set_workers_per_node;
		wper_cpu = siq_gc->root->bwt->workers_per_cpu;
		max_wpn_per_job = siq_gc->root->bwt->workers_per_node_per_job;
		goto out;
	}

	/* Save previous values to figure out what changed */
	prev_wper_node = max_wper_node;
	prev_wper_cluster = max_wper_cluster;
	prev_wper_job = max_wper_job;
	prev_set_wper_node = set_wper_node;
	prev_wper_cpu = wper_cpu;
	prev_wpn_per_job = max_wpn_per_job;

	/* Get new values */
	max_wper_node = siq_gc->root->bwt->max_workers_per_node;
	max_wper_cluster = siq_gc->root->bwt->max_workers_per_cluster;
	max_wper_job = siq_gc->root->coordinator->max_wperpolicy;
	set_wper_node = siq_gc->root->bwt->set_workers_per_node;
	wper_cpu = siq_gc->root->bwt->workers_per_cpu;
	max_wpn_per_job = siq_gc->root->bwt->workers_per_node_per_job;

	/* Set to safe values if they don't make sense. These values should
	 * only ever be changed by support but let's be safe. -1 means
	 * unlimited or dynamic caps */
	if (max_wper_node <= 0 && max_wper_node != -1)
		max_wper_node = 1;
	if (max_wper_cluster <= 0 && max_wper_cluster != -1)
		max_wper_cluster = 1;
	if (max_wper_job <= 0 && max_wper_job != -1)
		max_wper_job = 1;
	if (wper_cpu > MAX_WORKERS_PER_CPU) {
		log(ERROR, "Workers per CPU cannot be set higher than 20.");
		wper_cpu = 20;
	} else if (wper_cpu <= 0 && wper_cpu != -1) {
		wper_cpu = 1;
	}
	if (max_wpn_per_job <= 0 && max_wpn_per_job != -1)
		max_wpn_per_job = 1;

	/* Update node limits and reallocate */
	if (set_wper_node != prev_set_wper_node ||
	    wper_cpu != prev_wper_cpu) {
		workers_changed = true;
		work_ctx.update_node_info = true;
		work_ctx.update_node_rations = true;
		goto out;
	}

	/* Just reallocate */
	if (max_wper_node != prev_wper_node ||
	    max_wper_cluster != prev_wper_cluster ||
	    max_wper_job != prev_wper_job ||
	    max_wpn_per_job != prev_wpn_per_job) {
		workers_changed = true;
		work_ctx.update_node_rations = true;
	}

out:
	if (workers_changed)
		reallocate(&work_ctx);
}

static int
conf_reload(void *ctx)
{
	open_log(&cpu_ctx);
	conf_check(&cpu_ctx, true);

	if (g_ctx.master) {
		open_log(&bw_ctx);
		conf_check(&bw_ctx, true);

		open_log(&th_ctx);
		conf_check(&th_ctx, true);

		open_log(&work_ctx);
		conf_check(&work_ctx, true);

		reload_global_conf(false);
	}
	return 0;
}

/* Assumes MCP sends us SIGHUP on config update */
static void
sighup(__unused int signo)
{
	struct timeval tv = {};
	migr_register_timeout(&tv, conf_reload, NULL);
}

/*  __  __       _       
 * |  \/  | __ _(_)_ __  
 * | |\/| |/ _` | | '_ \ 
 * | |  | | (_| | | | | |
 * |_|  |_|\__,_|_|_| |_|
 */

static void
usage(void)
{
	printf("usage: " PROGNAME " [-d] [-l loglevel]\n");
	exit(EXIT_FAILURE);
}

int
main(int argc, char **argv)
{
	char c;
	bool debug = false;
	int cpu_listen_sock = -1;

	struct ilog_app_init init = {
		.full_app_name = "siq_bandwidth",
		.default_level = IL_FATAL,
		.use_syslog = true,
		.syslog_facility = LOG_DAEMON,
		.syslog_program = "isi_migrate",
		.log_file= "",
		.syslog_threshold = IL_INFO_PLUS, /////IL_TRACE_PLUS  ->  IL_INFO_PLUS
		.component = "bandwidth",
		.job = "",
	};
	
	ilog_init(&init, false, true);
	set_siq_log_level(INFO);

	reload_global_conf(true);

	if (access(SIQ_SCHED_WORKING_DIR, F_OK) < 0 &&
	    mkdir(SIQ_SCHED_WORKING_DIR, S_IRWXU | S_IRWXG | S_IRWXO) 
	    && errno != EEXIST) {
		log(FATAL, "Failed to create app work dir %s, %s", 
		    SIQ_SCHED_WORKING_DIR, strerror(errno));
	}

	if (access(SIQ_BWT_WORKING_DIR, F_OK) < 0 &&
	    mkdir(SIQ_BWT_WORKING_DIR, S_IRWXU | S_IRWXG | S_IRWXO) &&
	    errno != EEXIST) {
		log(FATAL, "Failed to create daemon work dir %s, %s", 
		    SIQ_BWT_WORKING_DIR, strerror(errno));
	}

	if (UFAIL_POINT_INIT("isi_migr_bandwidth", isi_migr_bandwidth_ufp) < 0)
		log(FATAL, "Failed to init ufp (%s)", strerror(errno));

	get_version_status(true, &g_ctx.ver_stat);

	init_ctx(&bw_ctx);
	init_ctx(&th_ctx);
	init_ctx(&cpu_ctx);
	init_ctx(&work_ctx);

	//Worker pool init
	uint16_set_init(&(work_ctx.isi_stats_keys));
	uint16_set_add(&(work_ctx.isi_stats_keys), isk_node_cpu_count);
	uint16_set_add(&(work_ctx.isi_stats_keys), isk_node_disk_count);
	work_ctx.update_node_info = true;
	work_ctx.update_node_rations = true;

	if (argc == 2 && !strcmp(argv[1], "-v")) {
		siq_print_version(PROGNAME);
		goto out;
	}

	while ((c = getopt(argc, argv, "dl:")) != -1) {
		switch (c) {
		case 'd':
			debug = true;
			break;
		case 'l':
			set_siq_log_level_str(optarg);
			break;
		default:
			usage();
		}
	}

	argc -= optind;
	argv += optind;

	if (argc)
		usage();

	if (!debug)
		daemon(0, 0);

	check_for_dont_run(true);

	signal(SIGHUP, sighup);
	signal(SIGUSR2, sigusr2);

	/* BWT daemon can come up before flexnet is ready. Wait for it. */
	while (!flx_config_exists()) {
		log(DEBUG, "Flexnet config file does not yet exist");
		siq_nanosleep(10, 0);
	}

	g_ctx.devid = migr_register_group_callback(group_cb, NULL);////g_ctx.devid = sysctl "efs.rbm.array_id"
	migr_register_flexnet_callback(group_cb, NULL);////通过isi_event来注册group_cb函数,如果有event,会调用group_cb.重启node,会回调group_cb消息?

	g_ctx.master = false;
	g_ctx.master_fd = -1;
	g_ctx.ip = NULL;

	/* check to see if we're the master at startup */
	group_cb(NULL);

	/* all bwt daemons (master or otherwise) are responsible for cpu
	 * throttling of workers on the same node */
	/////所有node都会在cpu_port(3149)上监听,接收自己node的cpu信息
	if ((cpu_listen_sock = listen_on(siq_gc->root->bwt->cpu_port, true))
	    < 0)
		log(FATAL, "failed to create socket");
	migr_add_listen_fd(cpu_listen_sock, NULL, "listen");
	migr_register_callback(cpu_listen_sock, CONN_MSG, cpu_connect_cb);

	migr_register_timeout(&timeout, timer_cb, NULL);

	migr_process();

out:
	return 0;
}

