ISI_TOP=	../../..

.include "${ISI_TOP}/bin/isi_migrate/local/Makefile.tsm"

PROG=		isi_migr_pworker
SRCS=		pworker.c handle_dir.c pw_diff_sync.c stf_based.c change_compute.c domain_mark.c compliance_stf_based.c
MAN=
NO_PROFILE=	1
#XXXDPL Remove CLOUDPOOLS_ENABLED when enabled by default.
CFLAGS+=	-DPROGNAME=\"${PROG}\" -DCLOUDPOOLS_ENABLED

LDFLAGS+=	${LOCAL_LDFLAGS}

LIBADD+=	ifs
LIBADD+=	isi_acl
LIBADD+=	isi_burst_sdk
LIBADD+=	isi_cloud_api
LIBADD+=	isi_cpool_bkup
LIBADD+=	isi_cpool_cbm
LIBADD+=	isi_cpool_config
LIBADD+=	isi_cpool_d_common
LIBADD+=	isi_domain
LIBADD+=	isi_gmp
LIBADD+=	isi_ilog
LIBADD+=	isi_migrate
LIBADD+=	isi_rest_server
LIBADD+=	isi_snapshot
LIBADD+=	isi_ufp
LIBADD+=	isi_util

LDADD+=	-lcurl
LDADD+=	-lcrypto
LDADD+=	-lmd

CHECK_SRCS=	change_compute.c		\
		handle_dir.c			\
		stf_based.c			\
		pw_diff_sync.c			\
		pworker.c			\
		domain_mark.c			\
		test_helper.c			\
		compliance_stf_based.c		\
		check_file_split.c		\
		check_cc_start.c		\
		check_cc_worklist.c		\
		check_cc_restore.c
#		check_cc_transfer.c		\

CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_testutil -lisi_testutil

.if !defined(COV_CHECK_TESTS) || ${COV_CHECK_TESTS} == 0
COVFILTERED  = test_helper.*
COVFILTERED += test_repstate.*
COVFILTERED += ls_cookies.*
.endif

test_repstate: test_helper.c test_repstate.c
	${CC} ${CFLAGS} ${.ALLSRC} -o ${.TARGET} ${CHECK_LIBS}

ls_cookies: ls_cookies.c
	${CC} ${CFLAGS} ${.ALLSRC} -o ${.TARGET} ${CHECK_LIBS}

check_lin_open: check_lin_open.c
	${CC} ${CFLAGS} ${.ALLSRC} -o ${.TARGET} ${CHECK_LIBS}

try_split: try_split.c
	${CC} ${CFLAGS} ${.ALLSRC} -o ${.TARGET} ${CHECK_LIBS}

stf_diff: stf_diff.c change_compute.c
	${CC} ${CFLAGS} ${.ALLSRC} -o ${.TARGET} ${CHECK_LIBS}

.include "${ISI_TOP}/isi.prog.mk"
