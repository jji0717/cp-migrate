ISI_TOP=	../../..

.include "${ISI_TOP}/bin/isi_migrate/local/Makefile.tsm"

PROG=		isi_migrate
MAN=
NO_PROFILE=	1

SRCS+=		conf.c
SRCS+=		coord.c
SRCS+=		coord_snapshot.c
SRCS+=		coord_worker.c
SRCS+=		coord_connect.c
SRCS+=		coord_fofb.c

CFLAGS+=	-DPROGNAME=\"${PROG}\"
CFLAGS+=        -I${.CURDIR:H:H:H}/lib/isi_migrate/config
CFLAGS+=        -I${.OBJDIR:H:H:H}/lib/isi_migrate/config

LDFLAGS+=       ${LOCAL_LDFLAGS}

LIBADD+=	isi_cloud_api
LIBADD+=	isi_cpool_cbm
LIBADD+=	isi_cpool_config
LIBADD+=	isi_cpool_d_common
LIBADD+=	isi_domain
LIBADD+=	isi_gmp
LIBADD+=	isi_ilog
LIBADD+=	isi_migrate
LIBADD+=	isi_snapshot
LIBADD+=	isi_sra_gcfg
LIBADD+=	isi_ufp
LIBADD+=	isi_upgrade_api
LIBADD+=	isi_util
LIBADD+=	ifs

CHECK_SRCS+=	conf.c				\
		coord.c				\
		coord_connect.c			\
		coord_snapshot.c		\
		coord_worker.c			\
		coord_fofb.c			\
		check_fofb.c			\
		check_stat.c			\
		check_cb.c			\
#		check_conf.c			\
#		check_dwa.c			\ #TODO: Fix invalid tests

NO_WERROR=	bug 150120

.include "${ISI_TOP}/isi.prog.mk"
