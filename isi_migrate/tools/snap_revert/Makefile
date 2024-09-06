ISI_TOP=	../../../..

TSM_LDADD=	1
.include "${ISI_TOP}/bin/isi_migrate/local/Makefile.tsm"

PROG=		snap_revert
SRCS=		snap_revert.c
NO_PROFILE=	1
MAN=

CFLAGS+=	-DPROGNAME=\"${PROG}\"

LDFLAGS+=       ${LOCAL_LDFLAGS}

LIBADD+=	isi_migrate

.include "${ISI_TOP}/isi.prog.mk"
