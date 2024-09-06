ISI_TOP=	../../../..

.include "${ISI_TOP}/bin/isi_migrate/local/Makefile.tsm"

PROG=		isi_changelist_mod
SRCS=		changelist_mod.c
BINDIR=		/usr/bin
MAN=
NO_PROFILE=	1
CFLAGS+=	-DPROGNAME=\"${PROG}\"

LDFLAGS+=       ${LOCAL_LDFLAGS}

LIBADD+=	isi_changelist
LIBADD+=	isi_migrate
LIBADD+=	isi_util

.include "${ISI_TOP}/isi.prog.mk"
