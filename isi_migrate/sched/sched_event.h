#ifndef SCHED_EVENT_H
#define SCHED_EVENT_H


void init_event_queue(struct sched_ctx *, struct isi_error **);
void wait_for_event(struct sched_ctx *, int, struct isi_error **);
bool set_proc_watch(struct sched_ctx *, int, struct isi_error **);


#endif
