ISI_TOP=../..

PROG_CXX=	isi_cpool_d
BINDIR=		/usr/sbin

CXXFLAGS+=	-fPIC
MAN=

CXXFLAGS+=	${LOCAL_CFLAGS}
CXXFLAGS+=	${FSP_CFLAGS}
CXXFLAGS+= -g

LDFLAGS+=	${LOCAL_LDFLAGS}
LDFLAGS+=	${FSP_LDFLAGS}

LIBADD+=	isi_rest_server
LIBADD+=	isi_sbtree
LIBADD+=	isi_cloud_common
LIBADD+=	isi_cpool_d_common
LIBADD+=	isi_cpool_d_api
LIBADD+=	isi_cpool_d
LIBADD+=	isi_cpool_cbm
LIBADD+=	isi_cpool_config
LIBADD+=	isi_cloud_api
LIBADD+=	isi_cpool_security
LIBADD+=	isi_cloud_api
LIBADD+=	isi_celog
LIBADD+=	isi_daemon
LIBADD+=	isi_gmp
LIBADD+=	isi_ilog
LIBADD+=	isi_cpool_security
LIBADD+=	isi_ufp
LIBADD+=	isi_util
LIBADD+=	isi_util_cpp
LIBADD+=	isi_licensing

LDADD+=		-llwds
LDADD+=		-lpthread

SRCS=
SRCS+=		main.cpp

.include "${ISI_TOP}/isi.prog.mk"
