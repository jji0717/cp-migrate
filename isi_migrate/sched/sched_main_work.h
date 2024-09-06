#ifndef SCHED_MAIN_WORK_H
#define SCHED_MAIN_WORK_H

struct sched_ctx;

int sched_main_node_work(struct sched_ctx *);

bool get_schedule_info(char *schedule, char **recur_str, time_t *base_time);

/*
 * We scan report folder to remove older report logs 
 * every ROTATE_REPORT_INTERVAL. This is currently 
 * defined as 1 day. This is due to fact that minimum
 * resolution is 1 day
 */
#define ROTATE_REPORT_INTERVAL (60 * 60 *24)

#endif /* SCHED_MAIN_WORK_H */
