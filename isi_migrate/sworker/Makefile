ISI_TOP=	../../..

.include "${ISI_TOP}/bin/isi_migrate/local/Makefile.tsm"

PROG=		isi_migr_sworker
SRCS=		sworker.c legacy.c delete.c sw_diff_sync.c tmonitor.c stf_transfer.c stf_quota_support.c tmp_utils.c compliance_sworker.c
MAN=
NO_PROFILE=	1
CFLAGS+=	-DPROGNAME=\"${PROG}\"
CFLAGS+=        -I${.CURDIR:H:H:H}/lib/isi_migrate/config
CFLAGS+=        -I${.OBJDIR:H:H:H}/lib/isi_migrate/config
#XXXDPL Remove CLOUDPOOLS_ENABLED when enabled by default.
CFLAGS+=	-DCLOUDPOOLS_ENABLED

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
LIBADD+=	isi_licensing
LIBADD+=	isi_migrate
LIBADD+=	isi_persona
LIBADD+=	isi_quota
LIBADD+=	isi_rest_server
LIBADD+=	isi_snapshot
LIBADD+=	isi_ufp
LIBADD+=	isi_util

LDADD+=	-lcurl
LDADD+=	-lcrypto
LDADD+=	-lmd

CHECK_LIBS+=	${LDADD}
#CHECK_LIBS+=    -L${ISI_TOP}/lib/isi_migrate -lisi_migrate
CHECK_SRCS+=	${SRCS}
#CHECK_SRCS+=	check_sparse.c
#CHECK_SRCS+=	check_del.c
#CHECK_SRCS+=	check_lists.c
#CHECK_SRCS+=	check_linmap.c
CHECK_SRCS+=	check_utils.c
CHECK_SRCS+=	check_stfcalls.c
CHECK_SRCS+=	check_quotas.c
CHECK_SRCS+=	check_multi_tmp.c
CHECK_SRCS+=	check_tree_mv.c
CHECK_SRCS+=	check_target_work.c

.if !defined(COV_CHECK_TESTS) || ${COV_CHECK_TESTS} == 0
COVFILTERED  = fake_cancel.*
COVFITLERED += sw_check_helper.*
.endif

fake_cancel: fake_cancel.o
	$(CC) $(CFLAGS) fake_cancel.o -o fake_cancel $(LDFLAGS) $(LDADD)

pid_check: pid_check.o
	$(CC) $(CFLAGS) pid_check.o -o pid_check $(LDFLAGS) $(LDADD)

NO_WERROR=	bug 150120

.include "${ISI_TOP}/isi.prog.mk"
