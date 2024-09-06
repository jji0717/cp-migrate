ISI_TOP=	../../..

.include "${ISI_TOP}/bin/isi_migrate/local/Makefile.tsm"

PROG=		isi_migr_bandwidth
CFLAGS+=	-DPROGNAME=\"${PROG}\"
SRCS=		bandwidth.c
CHECK_SRCS=	check_cpu_throttle.c
CHECK_SRCS=	check_worker_throttle.c
MAN=
NO_PROFILE=	1

LDFLAGS+=       ${LOCAL_LDFLAGS}

LIBADD+=	isi_flexnet
LIBADD+=	isi_ilog
LIBADD+=	isi_migrate
LIBADD+=	isi_stats
LIBADD+=	isi_ufp
LIBADD+=	isi_util

.if !defined(COV_CHECK_TESTS) || ${COV_CHECK_TESTS} == 0
COVFILTERED = generator.c
.endif

NO_WERROR=  bug 150120

.include "${ISI_TOP}/isi.prog.mk"

generator: generator.o
	${CC} ${CFLAGS} -o ${.TARGET} ${.ALLSRC} ${CHECK_LIBS}
