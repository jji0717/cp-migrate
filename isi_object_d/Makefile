ISI_TOP=../..


PROG_CXX=	isi_object_d
BINDIR=		/usr/sbin
MAN=
CXXFLAGS+=	-g
CXXFLAGS+=	-fPIC
CXXFLAGS+=	${LOCAL_CFLAGS}
CXXFLAGS+=	${FSP_CFLAGS}

LDFLAGS+=	${LOCAL_LDFLAGS}
LDFLAGS+=	${FSP_LDFLAGS}

LIBADD+=	ifs
LIBADD+=	isi_daemon
LIBADD+=	isi_gconfig
LIBADD+=	isi_gmp
LIBADD+=	isi_ilog
LIBADD+=	isi_ntoken
LIBADD+=	isi_object_api
LIBADD+=	isi_rest_server
LIBADD+=	isi_util

LDADD+=		-ljsoncpp
LDADD+=		-lfcgi
LDADD+=		-lz
LDADD+=		-ldevstat
LDADD+=		-lutil
LDADD+=		-lcrypto
LDADD+=		-lprotobuf
LDADD+=		-lpthread

SRCS=
SRCS+=		main.cpp

NO_WERROR=	bug 150004

.include "${ISI_TOP}/isi.prog.mk"
