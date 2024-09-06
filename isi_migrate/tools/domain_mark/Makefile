ISI_TOP=	../../../..

.include "${ISI_TOP}/bin/isi_migrate/local/Makefile.tsm"

PROG=		domain_mark
SRCS=		domain_mark.c
NO_PROFILE=	1
MAN=

CFLAGS+=	-DPROGNAME=\"${PROG}\"

LDFLAGS+=       ${LOCAL_LDFLAGS}

LIBADD+=	isi_migrate
LIBADD+=	isi_util

.include "${ISI_TOP}/isi.prog.mk"
