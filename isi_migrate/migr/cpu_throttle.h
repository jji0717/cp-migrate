#ifndef CPU_THROTTLE_H
#define CPU_THROTTLE_H


#include <stdbool.h>
#include "isirep.h"


struct migr_cpu_throttle_ctx {
	bool initialized;
	int host;
	int num_cores;
	uint32_t ration;			/* cycles per-thousand */
	uint64_t period_start_time;		/* usec since epoch */
	uint64_t period_cpu_start_time;		/* usec since epoch */
	uint16_t port;
	enum worker_type type;

	/* stat tracking */   ////////bandwidth统计总的 cpu_total = cpu_time_elapsed / time_elapsed; cpu利用率
	uint64_t time_elapsed;			/* usec */
	uint64_t cpu_time_elapsed;		/* usec */
};


void cpu_throttle_ctx_init(struct migr_cpu_throttle_ctx *cpu_ctx,  //////pworker.c sworker.c 的main中初始化调用
    enum worker_type type, uint16_t cpu_port);
void connect_cpu_throttle_host(struct migr_cpu_throttle_ctx *cpu_ctx,////pworker.c 中的timer_cvallback中调用.如果之前没连接过,就连接一下
    char *component);
void send_cpu_stat_update(struct migr_cpu_throttle_ctx *cpu_ctx);
void cpu_throttle_start(struct migr_cpu_throttle_ctx *cpu_ctx);/////收到work itit callback时调用
void print_cpu_throttle_state(FILE *f, struct migr_cpu_throttle_ctx *th);


#endif /* CPU_THROTTLE_H */
