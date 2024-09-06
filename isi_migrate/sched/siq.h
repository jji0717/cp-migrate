#ifndef SIQ_H
#define SIQ_H

#define POLICY_ID_LEN	32

#undef  TRUE
#define TRUE		1

#undef  FALSE
#define FALSE		0

#define FAIL		(-1)
#define SUCCESS		0

#define ISI_ROOT	"/ifs/.ifsvar/modules"

#define SCHED_ALWAYS	"when-source-modified"
#define SCHED_SNAP	"when-snapshot-taken"

#define SIQ_BASE_INTERVAL		10  //  In seconds.
#define CHANGE_BASED_POLICY_INTERVAL	SIQ_BASE_INTERVAL
#define SNAP_TRIG_POLICY_INTERVAL	SIQ_BASE_INTERVAL
#define RPO_ALERT_POLICY_INTERVAL	SIQ_BASE_INTERVAL

#ifdef _DEBUG_
#define SIQ_DAEMON(nochdir, noclose)	0
#else /* NO _DEBUG_ */
#define SIQ_DAEMON(nochdir, noclose)	daemon(nochdir, noclose)
#endif /* _DEBUG_ */

#endif /* SIQ_H */
