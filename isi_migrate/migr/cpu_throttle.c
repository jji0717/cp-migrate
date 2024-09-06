#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/resource.h>
#include <sys/sysctl.h>
#include <isi_util/isi_assert.h>
#include "isi_migrate/sched/siq_log.h"
#include "cpu_throttle.h"

#define MAX_SLEEP_TIME 1000000		/* 1s */
#define THROTTLE_INTERVAL 100000	/* 100 ms */
#define DEFAULT_NUM_CORES 4

static struct migr_cpu_throttle_ctx *g_cpu_ctx = NULL;


static inline uint64_t
timeval_to_usec(struct timeval tv)
{
	return tv.tv_sec * 1000000 + tv.tv_usec;
}


static inline uint64_t
get_time_usec(void)
{
	struct timeval tv;
	struct timespec ts;
	int ret;

	ret = clock_gettime(CLOCK_UPTIME, &ts);
	ASSERT(ret != -1);
	TIMESPEC_TO_TIMEVAL(&tv, &ts);

	return timeval_to_usec(tv);
}


static inline uint64_t
get_cpu_time_usec(int num_cores/*8个core*/)
{
	struct rusage ru;
	int ret;
	uint64_t cpu_time;
	///who:统计资源的对象,有如下取值
	///RUSAGE_SELF: 返回调用进程(当前这个pworker)的资源使用统计信息,即该进程中所有线程使用的资源总和
	///RUSAGE_THREAD: 返回调用线程的资源使用统计信息
	///RUSAGE_CHILDREN: 返回调用进程所有已终止且被回收子进程的资源使用统计信息
	////get resource usage: 进程资源统计函数(线程安全的).用于统计系统资源使用情况,即进程执行直到调用该函数时的资源使用情况.
	///如果在不同的时间调用该函数,会得到不同的结果 #include <sys/resource.h>
	ret = getrusage(RUSAGE_SELF, &ru);
	ASSERT(ret != -1);
	////ru_utime:返回进程在用户模式下的执行时间,以timeval结构的形式返回
	///ru_stime:返回进程在内核模式下的执行时间,以timeval结构的形式返回
	cpu_time = timeval_to_usec(ru.ru_utime) + timeval_to_usec(ru.ru_stime);

	/* The cpu usage time represents the usage on only one
	 * core (pworkers/sworkers are single threaded). We need to divide
	 * by the number of cores to get the overall cpu time. */
	if (cpu_time > 0)
		cpu_time /= num_cores/*8*/;////因为统计出来的cpu_time是整个cpu的,而cpu有8个核.一个pworker仅占一个core,所以要/8

	return cpu_time;
}

static uint64_t
calculate_sleep_time(uint64_t period_elapsed/*上次至今经过的时间*/, uint64_t period_cpu_use/*上次至今cpu使用的时间*/,
    uint32_t ration)/////ration:  total(cpu_time/time) / worker_nums ;
{
	uint64_t sleep_time = 0;

	/* zero cpu ration is illegal */
	ASSERT(ration > 0);

	/* Don't need to sleep if no ration */
	if (ration == 1000)
		goto out;

	/* Only sleep if we exceeded our ration */
	if (period_cpu_use >
	    period_elapsed * ration / 1000) {
		/* sleep for an amount of time such that the cpu use divided
		 * by the total period length, including sleep, meets the
		 * cpu ration */
		sleep_time = (period_cpu_use * 1000 / ration) - period_elapsed; /////sleep_time = cpu使用时间 / cpu使用率 - period_elapsed   cpu使用时间/cpu使用率 相当于总时间

		if (sleep_time > MAX_SLEEP_TIME)
			sleep_time = MAX_SLEEP_TIME;
	}

out:
	return sleep_time;
}

/* Remember that this function is inside of a signal handler. Future
 * modifications should be careful and avoid modifying any global/heap values
 * that are used in other components of the program. Modifying such variables
 * in a signal handler is a bad idea as they might be in the middle of being
 * used when this handler is called. */
static void
cpu_throttle(struct migr_cpu_throttle_ctx *cpu_ctx)////定时器函数
{
	uint64_t period_elapsed;
	uint64_t period_cpu_use;
	uint64_t sleep_time;

	/* time elapsed so fat this period */ ////上次统计后到现在所经过的时间
	period_elapsed = get_time_usec() -
	    cpu_ctx->period_start_time;

	/* microseconds this process used CPU since last period ended. */
	period_cpu_use = get_cpu_time_usec(cpu_ctx->num_cores) - //上次统计后到现在这个进程上的(pworker)cpu所使用的时间
	    cpu_ctx->period_cpu_start_time;

	/* sleep enough to adhere to our ration *////根据bandwidth update后的ration决定sleep多久
	sleep_time = calculate_sleep_time(period_elapsed, period_cpu_use,
	    cpu_ctx->ration);
	
	if (sleep_time > 0)
		siq_nanosleep(0, sleep_time * 1000);

	/* start new period */
	cpu_ctx->period_start_time += period_elapsed + sleep_time;
	cpu_ctx->period_cpu_start_time += period_cpu_use;

	/* update stats */
	cpu_ctx->time_elapsed += period_elapsed + sleep_time;
	cpu_ctx->cpu_time_elapsed += period_cpu_use;
}


static int/////bandwidth.c中的reallocate_cpu,把更新后的per_worker_ration发过来
cpu_throttle_callback(struct generic_msg *m, void *ctx)
{
	struct migr_cpu_throttle_ctx *cpu_ctx = ctx;
	uint32_t new_ration = m->body.cpu_throttle.ration;

	/* zero cpu ration is illegal */
	ASSERT(new_ration > 0);
	ASSERT(new_ration <= 1000);

	/* only log at debug if ration changed, always log at trace */
	if (cpu_ctx->ration != new_ration) {
		log(DEBUG, "%s: ration updated from %d/1000 to %d/1000",
		    __func__, cpu_ctx->ration, new_ration);
	} else {
		log(TRACE, "%s: ration unchanged. Old ration: %d/1000 new "
		    "ration: %d/1000", __func__, cpu_ctx->ration, new_ration);
	}

	cpu_ctx->ration = new_ration;

	return 0;
}


static int
cpu_throttle_disconnect_callback(struct generic_msg *m, void *ctx)
{
	struct migr_cpu_throttle_ctx *cpu_ctx = ctx;

	log(TRACE, "%s", __func__);

	migr_rm_fd(cpu_ctx->host);
	close(cpu_ctx->host);
	cpu_ctx->host = -1;
	
	/* what do we do when the connection is lost? we cant set the ration
	 * to zero and pause in the throttle function since we'd become
	 * unresponsive. so do we leave the current ration and let the worker
	 * notice the problem (fd == -1) and decide what to do (i.e. pause)? */

	return 0;
}

static int
get_num_cores(void)//////使用linux的系统调用函数:sysctl  /proc/cpuinfo中的cpu cores字段
{
	int num_cores, mib[3];
	size_t len = 3;

	if (sysctlnametomib("kern.smp.cpus", mib, &len) != 0) {////node上:sysctl -a | grep kern.smp.cpus  kern.smp.cpus: 8
		return DEFAULT_NUM_CORES;
	}

	/* This shouldn't fail, but fall back to a reasonable default value
	 * if it does. */
	len = sizeof(num_cores);
	if (sysctl(mib, 3, &num_cores, &len, NULL, 0) != 0) {
		num_cores = DEFAULT_NUM_CORES;
	}

	return num_cores;
}

void ////pworker连接bandwidth成功后,回调这个函数
cpu_throttle_ctx_init(struct migr_cpu_throttle_ctx *cpu_ctx,
    enum worker_type type, uint16_t cpu_port)
{
	log(TRACE, "%s", __func__);

	ASSERT(!cpu_ctx->initialized);

	/* 0.1% ration until we get first update from host */
	cpu_ctx->ration = 1;
	cpu_ctx->host = -1;
	cpu_ctx->num_cores = get_num_cores();
	cpu_ctx->period_start_time = get_time_usec();
	cpu_ctx->period_cpu_start_time = get_cpu_time_usec(cpu_ctx->num_cores);
	cpu_ctx->time_elapsed = 0;
	cpu_ctx->cpu_time_elapsed = 0;
	cpu_ctx->port = cpu_port;
	cpu_ctx->type = type;
	cpu_ctx->initialized = true;
}


void////pworker中的timer_callback中调用
send_cpu_stat_update(struct migr_cpu_throttle_ctx *cpu_ctx)
{
	struct generic_msg msg = {};

	log(TRACE, "%s", __func__);

	ASSERT(cpu_ctx->initialized);

	if (cpu_ctx->host == -1)
		goto out;

	/* don't send a stat update unless we've got new data */
	if (cpu_ctx->time_elapsed == 0)
		goto out;

	msg.head.type = CPU_THROTTLE_STAT_MSG;
	msg.body.cpu_throttle_stat.time = cpu_ctx->time_elapsed;
	msg.body.cpu_throttle_stat.cpu_time = cpu_ctx->cpu_time_elapsed;
	msg_send(cpu_ctx->host, &msg);

	cpu_ctx->time_elapsed = 0; /////已经发送过了,那么老的time_elapsed cpu_time_elapsed恢复
	cpu_ctx->cpu_time_elapsed = 0;

out:
	return;
}


static void
on_alarm(int sig)
{
	int tmp_errno = errno;

	ASSERT(g_cpu_ctx != NULL);

	cpu_throttle(g_cpu_ctx);
	errno = tmp_errno;
}


////unsigned int alarm(unsigned int seconds); 
////useconds_t ualarm(useconds_t usecs, useconds_t interval);
///在usecs微秒后,将SIGALRM信号发送给进程,并且之后每隔interval微秒再发送一次SIGALRM信号,如果不处理,默认终止进程
void
cpu_throttle_start(struct migr_cpu_throttle_ctx *cpu_ctx)
{
	struct sigaction alrm_act = {};

	log(TRACE, "%s", __func__);

	ASSERT(cpu_ctx->initialized);

	g_cpu_ctx = cpu_ctx;

	alrm_act.sa_handler = on_alarm;
	sigemptyset(&alrm_act.sa_mask);
	alrm_act.sa_flags = SA_RESTART;

	if (sigaction(SIGALRM, &alrm_act, NULL) == -1)
		log(FATAL, "Failed to establish SIGALRM handler: %s", strerror(errno));

	ualarm(THROTTLE_INTERVAL, THROTTLE_INTERVAL);
}


void
connect_cpu_throttle_host(struct migr_cpu_throttle_ctx *cpu_ctx,
    char *component)
{
	uint32_t ver;
	struct isi_error *error = NULL;
	struct generic_msg msg = {};
	struct version_status ver_stat = {};

	ASSERT(cpu_ctx->initialized);
	ASSERT(cpu_ctx->host == -1);

	log(TRACE, "%s", __func__);

	/* Use the most recently-committed cluster version for bw connect. */
	get_version_status(true, &ver_stat);

	/* probably want exponential backoff for connection attempts here */
	cpu_ctx->host = connect_to("localhost", cpu_ctx->port,/////连接同一台机器上的cpu throttl端口
	    POL_MATCH_OR_FAIL, ver_stat.committed_version, &ver, NULL, false,
	    &error);
	if (error) {
		log(DEBUG,
		    "failed to connect to cpu throttle host (localhost): %s",
		    isi_error_get_message(error));
		goto out;
	}

	log_version_ufp(ver, component, __func__);

	migr_add_fd(cpu_ctx->host, cpu_ctx, "cpu_throttle_host");
	migr_register_callback(cpu_ctx->host, CPU_THROTTLE_MSG,    //////bandwidth.c 发送调整后的cpu msg给每个worker
	    cpu_throttle_callback);
	migr_register_callback(cpu_ctx->host, DISCON_MSG,
	    cpu_throttle_disconnect_callback);

	msg.head.type = CPU_THROTTLE_INIT_MSG;
	msg.body.cpu_throttle_init.worker_type = cpu_ctx->type;
	msg_send(cpu_ctx->host, &msg);

out:
	isi_error_free(error);
	return;
}

void
print_cpu_throttle_state(FILE *f, struct migr_cpu_throttle_ctx *th)
{
	ASSERT(th != NULL);

	fprintf(f, "\n****CPU Throttle State****\n");
	fprintf(f, "  initialized: %d\n", th->initialized);
	fprintf(f, "  host: %d\n", th->host);
	fprintf(f, "  ration: %d cycles per 1000 (%.1f%%)\n", th->ration,
	    (float)th->ration / 10.0);
	fprintf(f, "  period_start_time: %llu\n", th->period_start_time);
	fprintf(f, "  period_cpu_start_time: %llu\n",
	    th->period_cpu_start_time);
	fprintf(f, "  port: %d\n", th->port);
	fprintf(f, "  worker_type: %d\n", th->type);
	fprintf(f, "  time_elapsed: %llu\n", th->time_elapsed);
	fprintf(f, "  cpu_time_elapsed: %llu\n", th->cpu_time_elapsed);
}

