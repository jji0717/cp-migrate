#ifndef SCHED_MAIN_H
#define SCHED_MAIN_H

/* Component */
#define SCHED_ID	"sched"

/* Files */
#define SCHED_JOB_CANCEL	"job_cancel"
#define SCHED_JOB_PAUSE		"job_pause"
#define SCHED_JOB_PREEMPT	"job_preempt"

/*
 * Queue manipulation macros.
 */
#define QUEUE_INIT(qh, type)			\
	(qh).next = (type *) &qh;		\
	(qh).prev = (type *) &qh
#define ENQUEUE_TAIL(qhp, item, type)		\
	(item)->next = (type *)(qhp);		\
	(item)->prev = (qhp)->prev;		\
	(qhp)->prev->next = (item);		\
	(qhp)->prev = (item)
#define ENQUEUE_HEAD(qhp, item, type)		\
	(item)->next = (qhp)->next;		\
	(item)->prev = (type *)(qhp);		\
	(qhp)->next->prev = (item);		\
	(qhp)->next = (item)
/*
 * Dequeue the element at the head of the queue.
 * - the queue must not be empty!
 */
#define DEQUEUE_HEAD(qhp, item, type)		\
	(item) = (qhp)->next;			\
	(item)->next->prev = (type *)(qhp);	\
	(qhp)->next = (item)->next;		\
	(item)->next = (item)->prev = NULL
/*
 * Dequeue an arbitrary element from a queue.
 * - 'item' must be set!
 * - we assume that 'item' is actually in the queue.
 */
#define DEQUEUE(qhp, item, type)		\
	(item)->prev->next = (item)->next;	\
	(item)->next->prev = (item)->prev;	\
	(item)->next = (item)->prev = NULL

#define QUEUE_EMPTY(qhp, type)		((qhp)->next == (type *)(qhp))
#define GETQUEUE_HEAD(qhp, item, type)			\
	do {						\
		(item) = NULL;				\
		if (!QUEUE_EMPTY(qhp, type)) {		\
			(item) = (qhp)->next;		\
			DEQUEUE(qhp, item, type);	\
		}					\
	} while (0)
#define GETQUEUE_TAIL(qhp, item, type)			\
	do {						\
		(item) = NULL;				\
		if (!QUEUE_EMPTY((qhp), (type))) {	\
			(item) = (qhp)->prev;		\
			DEQUEUE((qhp), (item), (type));	\
		}					\
	} while (0)
#define IS_LAST_ITEM(qhp, item, type)			\
	((item)->next == (type *)qhp)

#define SZSPRINTF(char_array, fmt, ...) do {                            \
	snprintf((char_array), sizeof(char_array), fmt, ##__VA_ARGS__); \
} while(0);

enum sched_reload_type {
	RELOAD_CONF = 0,
	RELOAD_NO,
	RELOAD_END
};

enum job_action_type {
	ACTION_NO_RUN,
	ACTION_END
};

enum sched_ret {
	SCH_YES,
	SCH_NO,
	SCH_RETRY,
	SCH_ERR,
	SCH_END
};

enum job_progress {
	PROGRESS_NONE = 0,
	PROGRESS_AVAILABLE = 1,
	PROGRESS_GRABBED = 2,
	PROGRESS_STARTED = 3,
};

typedef struct _iitp_intro {
	u_int32_t	version;
	u_int16_t	devid;
} iitp_intro;

typedef struct _iitp_group {
	u_int32_t	len;
	u_int16_t	devid;
	u_int16_t	pad1;
	u_int32_t	sid;
	u_int16_t	type;
	u_int16_t	pad2;
} iitp_group;

/* conf local (for node) params */
extern char	sched_edit_dir[];	/* "tsm/sched_edit/<node>" */
extern char	sched_edit_jobs_dir[];	/* "tsm/sched_edit/<node>/jobs" */
extern char	sched_edit_run_dir[];	/* "tsm/sched_edit/<node>/run" */
extern char	sched_test_file[];	/* "tsm/sched/<node>/test_file" */
/* conf local (for node) params end */

#endif /* SCHED_MAIN_H */
