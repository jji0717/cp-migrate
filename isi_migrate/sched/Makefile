ISI_TOP=	../../..

.include "${ISI_TOP}/bin/isi_migrate/local/Makefile.tsm"

PROG=   isi_migr_sched 
SRCS=	sched_local_work.c sched_main.c sched_main_work.c	\
	sched_utils.c sched_change_based.c sched_snap_trig.c	\
	sched_timeout.c sched_cluster.c				\
	sched_grabbed.c sched_event.c sched_delay.c

MAN=
NO_PROFILE=	1

CFLAGS+=	-I${.CURDIR:H:H:H}/lib/isi_migrate/config
CFLAGS+=	-I${.OBJDIR:H:H:H}/lib/isi_migrate/config
CFLAGS+=	-DPROGNAME=\"${PROG}\"
CFLAGS+=        -DCOORDNAME=\"/usr/bin/isi_migrate\"
CFLAGS+=        -DBUILD=\"${BUILD}\"

LDFLAGS+=       ${LOCAL_LDFLAGS}

LIBADD+=	ifs
LIBADD+=	isi_config
LIBADD+=	isi_date
LIBADD+=	isi_flexnet
LIBADD+=	isi_gmp
LIBADD+=	isi_ilog
LIBADD+=	isi_licensing
LIBADD+=	isi_migrate
LIBADD+=	isi_net
LIBADD+=	isi_sra_gcfg
LIBADD+=	isi_ufp
LIBADD+=	isi_util
LIBADD+=	isi_inx_addr

CHECK_SRCS=				\
		check_queue.c		\
		check_preempt.c		\
		check_needed.c		\
		check_grabbed.c		\
		check_timeout.c		\
		sched_local_work.c	\
		sched_grabbed.c		\
		sched_timeout.c		\
		sched_utils.c		\
		sched_event.c		\
		sched_cluster.c		\
		#check_job.c		\

CLEANFILES+=	tsm.log \
		isi_sched_d.test \
		isi_sched_d.result \
		check.log

CLEANDIRS+=	tsm

NO_WERROR=	bug 150120

foo: foo.c
	${CC} ${CFLAGS} ${.ALLSRC} -o ${.TARGET} ${CHECK_LIBS}

.include "${ISI_TOP}/isi.prog.mk"
